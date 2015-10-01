NAMESPACES = 'namespaces'
COLLECTIONS = 'collections'

DOC_ID_RE = r'(?P<doc_id>\w+?)'
NAMESPACE_RE = r'(?P<namespace>\w+?)'
COLLECTION_RE = r'(?P<collection>\w+?)'


def build_pattern(*segments):
    return '/{}/?'.format('/'.join(segments))
