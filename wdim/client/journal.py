class JournalEntry:

    @classmethod
    def from_document(cls, document: Dict[str, Any]) -> 'JournalEntry':
        return cls(
            document['_id'],
            document['collection'],
            document['namespace'],
            document['action'],
            document['blob'],
            schema=document['schema']
        )

    def __init__(self, _id, collection, namespace, action, blob, schema=None, validate=False):
        self._id = _id
        self._blob = blob
        self._action = action
        self._schema = schema
        self._namespace = namespace
        self._collection = collection

        if validate:
            self.get_schema().validate(self.get_blob().data)

    def to_document(self):
        return {
            '_id': self._id,
            'collection': self._collection,
            'namespace': self._namespace,
            'action': self._action,
            'blob': self._blob,
            'schema': self._schema
        }


