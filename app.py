# external imports
from flask import Flask
from flask_restful import Api
from flask_cors import CORS

# internal imports
from components.crawler import crawler_apis

app = Flask(__name__, static_folder = 'dist')
app.secret_key = '1adee321-15e5-4741-b19b-5287aeb9a13c'
CORS(app)

api = Api(app)

app.register_blueprint(crawler_apis.crawler_bp, url_prefix = '/crawler')

if __name__ == '__main__':
    app.run(host = '0.0.0.0', port = 8080, debug = True)