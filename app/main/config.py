import os

# uncomment the line below for postgres database url from environment variable
# postgres_local_base = os.environ['DATABASE_URL']

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'my_precious_secret_key')
    DEBUG = True
    # UPLOAD_FOLDER = "C:/Users/Wajdi"
    UPLOAD_FOLDER = "/scor-data"
    # MONGO_URI = "mongodb://root:Bxia!2020DaaTa1920CAvlmd@a4ec5441fc63a4fefbc97353d13465d2-1236515762.eu-west-3.elb.amazonaws.com:27017/dcm?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false&retryWrites=false"
    MONGO_URI = os.getenv("MONGO_URI")
    AWS_KEY = os.getenv("AWS_KEY")
    AWS_SECRET = os.getenv("AWS_SECRET")
    AWS_REGION = os.getenv("AWS_REGION")
    # AWS_KEY = "AKIAQHONPL2UAI43JG5S"
    # AWS_SECRET = "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"
    # AWS_REGION = "eu-west-3"

class DevelopmentConfig(Config):
    # uncomment the line below to use postgres
    # SQLALCHEMY_DATABASE_URI = postgres_local_base
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'flask_boilerplate_main.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class TestingConfig(Config):
    DEBUG = True
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'flask_boilerplate_test.db')
    PRESERVE_CONTEXT_ON_EXCEPTION = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class ProductionConfig(Config):
    DEBUG = True
    # uncomment the line below to use postgres
    # SQLALCHEMY_DATABASE_URI = postgres_local_base


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig
)

key = Config.SECRET_KEY
