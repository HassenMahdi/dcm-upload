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

    # MONGO_URI = 'mongodb://root:Bxia!2020DaaTa1920CAvlmd@a4ec5441fc63a4fefbc97353d13465d2-1236515762.eu-west-3.elb.amazonaws.com:27017/dcm?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false'
    MONGO_URI = os.getenv("MONGO_URI")
    AWS_KEY = os.getenv("AWS_KEY")
    AWS_SECRET = os.getenv("AWS_SECRET")
    AWS_REGION = os.getenv("AWS_REGION")

    AD_SUB_ID = 'b5bfc7e0-0306-4f8a-aacc-cefe8ebd78e8'
    AD_CLIENT_ID = "8c12706d-b53b-4de5-80df-1ea03deaf9d0"
    AD_SECRET = "QLwg2U1Rbp3oON~8W_N.~CsW9n-8tOssn6"
    AD_TENANT = "eede4321-0ccf-40f7-995d-271b1c0e60d3"

    ASA_URI = "BlobEndpoint=https://devdcmstorage.blob.core.windows.net/;" \
           "QueueEndpoint=https://devdcmstorage.queue.core.windows.net/;" \
           "FileEndpoint=https://devdcmstorage.file.core.windows.net/;" \
           "TableEndpoint=https://devdcmstorage.table.core.windows.net/;" \
           "SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-07-16T07:57:54Z&st=2020-07-15T23:57:54Z&spr=https&sig=4cDoQPv%2Ba%2FQyBEFcr2pVojyMj4vgsm%2Fld6l9TPveQH0%3D"

# BlobEndpoint=https://devdcmstorage.blob.core.windows.net/;SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-07-16T07:57:54Z&st=2020-07-15T23:57:54Z&spr=https&sig=4cDoQPv%2Ba%2FQyBEFcr2pVojyMj4vgsm%2Fld6l9TPveQH0%3D

class DevelopmentConfig(Config):
    # uncomment the line below to use postgres
    # SQLALCHEMY_DATABASE_URI = postgres_local_base
    DEBUG = True


class TestingConfig(Config):
    DEBUG = True
    TESTING = True


class ProductionConfig(Config):
    DEBUG = False
    # SECRET_KEY = os.getenv("SECRET_KEY")
    # UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER")
    # subscription_id = os.getenv("subscription_id")
    # client_id = os.getenv("client_id")
    # secret = os.getenv("secret")
    # tenant = os.getenv("tenant")
    # uncomment the line below to use postgres
    # SQLALCHEMY_DATABASE_URI = postgres_local_base


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig
)

key = Config.SECRET_KEY
