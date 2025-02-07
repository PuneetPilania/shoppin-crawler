# external imports
from flask import Blueprint, jsonify
from flask_restful import Resource, Api, reqparse
import json

# create Restful api object and Blueprint object
crawler_bp = Blueprint('crawler_api', __name__)
crawler_api = Api(crawler_bp)

# local imports
import db
from components.crawler.crawler_functions import CrawlerBuilder

class CrawelerOnApps(Resource):
    def post(self):
        conn = None
        try:
            # get all agrs from request
            parser = reqparse.RequestParser()
            parser.add_argument('process_name', type=str, required=True)
            parser.add_argument('domains', type=str, required=True)
            parser.add_argument('is_lazy_loading', type=int, required=False)

            args = parser.parse_args(strict=True)

            process_name = args['process_name']
            domains = json.loads(args['domains'])
            is_lazy_loading = args['is_lazy_loading']

            conn = db.db_connection()
            cursor = conn.cursor()
            
            # call crawl class builder to start process
            crawler_builder = CrawlerBuilder(conn, cursor, domain = domains, is_lazy_loading = is_lazy_loading)
            output = crawler_builder.domain_crawling_process_create(process_name)

            cursor.close()
            conn.close()

            return jsonify(output)
        except:
            if conn:
                conn.close()
            raise


class GetProducts(Resource):
    def post(self):
        conn = None
        try:
            # get all agrs from request
            parser = reqparse.RequestParser()
            parser.add_argument('process_id', type=str, required=True)

            args = parser.parse_args(strict=True)

            process_id = args['process_id']

            conn = db.db_connection()
            cursor = conn.cursor()
            
            # call crawl class builder to start process
            crawler_builder = CrawlerBuilder(conn, cursor, process_id=process_id)
            output = crawler_builder.get_process_domains_products()

            cursor.close()
            conn.close()

            return jsonify(output)
        
        except:
            if conn:
                conn.close()
            raise

crawler_api.add_resource(CrawelerOnApps, '/crawl_domain')
crawler_api.add_resource(GetProducts, '/process/domains/products/get')