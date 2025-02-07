import pymysql
from config import Shoppin

def db_connection():
    connection = pymysql.connect(host=Shoppin.Database.database_host,
                                 user=Shoppin.Database.user,
                                 password=Shoppin.Database.password,
                                 db=Shoppin.Database.database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor
                                 )

    return connection