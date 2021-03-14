import datetime

import jwt


from app.db.document import Document
from app.main import flask_bcrypt
from app.main.config import key


class User(Document):
    __TABLE__ = "users"

    email = None
    password_hash = None
    first_name = None
    last_name = None
    username = None
    userDb = None
    userNamedb = None
    dbPassword = None

    created_on = None
    modified_on = None

    admin = None
    roles = None


    # @property
    # def password(self):
    #     raise AttributeError('password: write-only field')
    #
    # @password.setter
    # def password(self, password):
    #     self.password_hash = flask_bcrypt.generate_password_hash(password).decode('utf-8')
    #
    # def check_password(self, password):
    #     return flask_bcrypt.check_password_hash(self.password_hash, password)
    #
    # @staticmethod
    # def encode_auth_token(user_id):
    #     """
    #     Generates the Auth Token
    #     :return: string
    #     """
    #     try:
    #         payload = {
    #             'exp': datetime.datetime.utcnow() + datetime.timedelta(days=1, seconds=5),
    #             'iat': datetime.datetime.utcnow(),
    #             'sub': user_id
    #         }
    #         return jwt.encode(
    #             payload,
    #             key,
    #             algorithm='HS256'
    #         )
    #     except Exception as e:
    #         return e
    #
    # @staticmethod
    # def decode_auth_token(auth_token):
    #     """
    #     Decodes the auth token
    #     :param auth_token:
    #     :return: integer|string
    #     """
    #     try:
    #         payload = jwt.decode(auth_token, key)
    #         is_blacklisted_token = BlacklistToken.check_blacklist(auth_token)
    #         if is_blacklisted_token:
    #             return 'Token blacklisted. Please log in again.'
    #         else:
    #             return dict(token=payload['sub'])
    #     except jwt.ExpiredSignatureError:
    #         return 'Signature expired. Please log in again.'
    #     except jwt.InvalidTokenError:
    #         return 'Invalid token. Please log in again.'
    #
    # def __repr__(self):
    #     return "<User '{} {}'>".format(self.first_name, self.last_name)