from wdim.server.api.v1 import document
from wdim.server.api.v1 import collection
from wdim.server.api.v1.schema import SchemaHandler

PREFIX = 'v1'
HANDLERS = (
    SchemaHandler.as_entry(),
    document.DocumentHandler.as_entry(),
    collection.CollectionHandler.as_entry()
)
