from app.db.document import Document


class ConnectorSetup(Document):
    __TABLE__ = "connectors"

    type=None
    password=None
    host=None
    user=None
    url=None
    port=None
    conn_string=None
    secret=None
    name=None
    created_on=None
    modified_on=None
    description=None
    database=None
    auth_with=None
    sas_token=None
    shared_access_key=None

