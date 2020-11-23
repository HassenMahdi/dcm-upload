import os

# uncomment the line below for postgres database url from environment variable
# postgres_local_base = os.environ['DATABASE_URL']

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'my_precious_secret_key')
    DEBUG = False
    UPLOAD_FOLDER = "/scor-data/"

    DATA_FOLDER = os.getenv("DATA_FOLDER", '/data-folder/')
    DRILL_SERVER = os.getenv("DRILL_SERVER", 'localhost')
    DRILL_PORT = os.getenv("DRILL_PORT", '8047')

    MONGO_URI = "mongodb://dcm-consmos:pUQRAZMYnTiYikWTxjcq7zQch27litMHCSJnHOu9XCssYxBqVRWmMpd8sSnd0G7w66dQ7GMS4UK8i" \
                "AvOsoBGtw==@dcm-consmos.mongo.cosmos.azure.com:10255/dcm?ssl=true&replicaSet=globaldb&retrywrites=" \
                "false&maxIdleTimeMS=120000&appName=@dcm-consmos@"

    subscription_id = 'b5bfc7e0-0306-4f8a-aacc-cefe8ebd78e8'
    client_id = "8c12706d-b53b-4de5-80df-1ea03deaf9d0"
    secret = "QLwg2U1Rbp3oON~8W_N.~CsW9n-8tOssn6"
    tenant = "eede4321-0ccf-40f7-995d-271b1c0e60d3"
    rg_name = 'Datacapture'
    df_name = 'dcm-factory'
    pip_name = "copyParquetToSql"


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
    DEBUG = False
    # uncomment the line below to use postgres
    # SQLALCHEMY_DATABASE_URI = postgres_local_base


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig
)

key = Config.SECRET_KEY
