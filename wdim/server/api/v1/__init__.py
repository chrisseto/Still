from wdim.server.api.v1 import auth
from wdim.server.api.v1 import document

PREFIX = 'v1'
HANDLERS = (
    auth.AuthHandler.as_entry(),
    document.HistoryHandler.as_entry(),
    document.DocumentHandler.as_entry(),
    document.DocumentsHandler.as_entry(),
)
