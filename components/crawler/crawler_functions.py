import asyncio, json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import tldextract
from collections import deque
from playwright.async_api import async_playwright
import aiohttp

from async_processes.producers.generic_producer import publish_event, create_rmq_connection
from config import Shoppin

class CrawlerBuilder:
    def __init__(self, conn, cursor, domain = None, is_lazy_loading=False, process_id = None, domain_id = None):
        self.conn = conn
        self.cursor = cursor
        self.domain = domain
        self.domain_id = domain_id
        self.process_id = process_id
        self.is_lazy_loading = is_lazy_loading
        self.max_pages = 80

        # local var
        self.visited = set()
        self.queue = deque([domain])
        self.product_links = set()

    def is_product_link(self, url):
        # to match products links
        keywords = ['product', 'item', 'detail', 'sku', 'buy', 'p/']
        return any(keyword in url.lower() for keyword in keywords)

    async def get_all_links(self, url):
        """
        Fetch all links from a URL asynchronously.
        """
        links = set()
        base_domain = tldextract.extract(url).registered_domain

        if self.is_lazy_loading:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                try:
                    await page.goto(url, timeout=60000)
                    await asyncio.sleep(1)
                    
                    # Scroll dynamically
                    last_height = await page.evaluate("document.body.scrollHeight")

                    for _ in range(3):  # Limit scroll attempts
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                        await asyncio.sleep(1)
                        new_height = await page.evaluate("document.body.scrollHeight")
                        if new_height == last_height:
                            break
                        last_height = new_height
                    
                    content = await page.content()
                    soup = BeautifulSoup(content, 'html.parser')
                finally:
                    await browser.close()
        else:
            try:
                headers = {'User-Agent': 'Mozilla/5.0'}
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, timeout=10) as response:
                        response.raise_for_status()
                        text = await response.text()
                        soup = BeautifulSoup(text, 'html.parser')
            except Exception as e:
                print(f"Error fetching {url}: {e}")
                return []

        # Extract links
        for a_tag in soup.find_all('a', href=True):
            link = urljoin(url, a_tag['href'])
            parsed_link = urlparse(link)
            if parsed_link.scheme.startswith('http') and base_domain in link:
                links.add(link)

        return list(links)
    
    async def crawl_page(self, url, n):
        print(f"Task {n} started")
        try:
            # fetch all links
            links = await self.get_all_links(url)

            # loop to filter product page and move on next hierarchy
            for link in links:
                if link not in self.visited:
                    if self.is_product_link(link):
                        self.product_links.add(link)
                    else:
                        self.queue.append(link)
        except:
            pass
        print(f"Task {n} completed")

        return True

    async def execute_queue(self):
        # loop over queue
        while self.queue and len(self.visited) < self.max_pages:
            select_func = []

            # collects tasks for parallel process
            for i in range(min(10, len(self.queue))):
                try:
                    url = self.queue.popleft()

                    # if already visited
                    if url in self.visited:
                        continue

                    # if max page exceed
                    if len(self.visited) > self.max_pages:
                        continue

                    print(f"In progress: {url}")

                    select_func.append(asyncio.create_task(self.crawl_page(url, i)))

                    self.visited.add(url)
                except:
                    pass
            
            # execute parallel tasks
            await asyncio.gather(*select_func)
        
        # convert result to list
        self.product_links = list(self.product_links)

        return True

    def start_crawling(self):
        # hit for crawl url
        asyncio.run(self.execute_queue())

    def domain_crawling_process_create(self, process_name):
        # if no domain requested
        if not len(self.domain):
            return {"status": 0, "message": "No domains requested."}

        # create a process
        sql = """
                INSERT INTO 
                    sc_processes(process_name, total_domains, is_lazy_loading_status) 
                    VALUES(%s, %s, %s)
            """
        self.cursor.execute(sql, (process_name, len(self.domain), self.is_lazy_loading))
        process_id = self.cursor.lastrowid

        # create domains for process
        sql = f"""
                INSERT INTO 
                    sc_domains(process_id, domain_name) 
                    VALUES({process_id}, %s)
            """
        self.cursor.executemany(sql, self.domain)
        self.conn.commit()
        
        # get all saved domain with id pk
        sql = """
            SELECT 
                id as domain_id, 
                domain_name 
            FROM sc_domains 
            WHERE process_id = %s 
        """
        self.cursor.execute(sql, (process_id, ))
        self.domain = self.cursor.fetchall()

        # create rmq connection
        rmq_connection, rmq_channel = create_rmq_connection()
        
        # produce message in crawling consumer
        for d in self.domain:
            message = d
            message['process_id'] = process_id
            message['is_lazy_loading'] = self.is_lazy_loading

            publish_event(Shoppin.Queue.rabbit_mq_url, Shoppin.Queue.crawl_domain_queue, message, rmq_channel_param=rmq_channel)
        
        rmq_connection.close()

        return {"status": 1, "message": "Crawling Started for domains", "data": {"process_id": process_id}}
    
    def save_crawling_results(self):
        # insert products
        sql =  f"""
            INSERT INTO 
                sc_domain_products(domain_id, product_url) 
                VALUES({self.domain_id}, %s)
        """
        self.cursor.executemany(sql, (self.product_links))
        
        # update product counts and status of domain
        sql = """
            UPDATE 
                sc_domains 
            SET 
                product_count = %s, 
                product_crawled_status = 1 
            WHERE 
                id = %s 
        """
        self.cursor.execute(sql, (len(self.product_links), self.domain_id))

        # increase count of progress of process
        sql = """
            UPDATE 
                sc_processes 
            SET 
                domain_progress_status = domain_progress_status + 1 
            WHERE 
                id = %s 
        """
        self.cursor.execute(sql, (self.process_id, ))

        self.conn.commit()

        return True
    
    def get_process_domains_products(self):
        # get all domains products
        sql = """
            SELECT 
                d.id as domain_id, 
                d.domain_name, 
                d.product_count,
                JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'product_id', p.id,
                        'product_url', p.product_url
                    )
                ) as products 
            FROM (
                SELECT 
                    id, domain_name, product_count  
                FROM sc_domains 
                WHERE 
                    process_id = %s 
                    AND delete_status = 0 
                    AND product_crawled_status = 1
            ) as d 
            INNER JOIN sc_domain_products p ON 
                p.domain_id = d.id 
                AND p.delete_status = 0
            GROUP BY d.id
        """
        self.cursor.execute(sql, (self.process_id, ))
        domain_products = self.cursor.fetchall()

        # return if no domains and products found
        if not len(domain_products):
            return {'status': 0, 'message': "No domains found"}
        
        for dp in domain_products:
            dp['products'] = json.loads(dp['products'])

        # return products
        return {'status': 1, 'message': "Products Fetched", "domain_products": domain_products}