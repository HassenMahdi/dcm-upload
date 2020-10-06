from flask_restplus import Api
from flask import Blueprint

from .main.controller.upload_controller import api as flow_ns
from .main.controller.tags_controller import api as tags_ns
from .main.controller.data_controller import api as data_ns

blueprint = Blueprint('api', __name__)

api = Api(blueprint,
          title='FLASK RESTPLUS API BOILER-PLATE WITH JWT',
          version='1.0',
          description='a boilerplate for flask restplus web service'
          )

api.add_namespace(flow_ns, path='/upload')
api.add_namespace(tags_ns, path='/upload')
api.add_namespace(data_ns, path='/upload')