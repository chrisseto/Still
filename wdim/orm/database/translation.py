import abc


class Translator(abc.ABC):

    @classmethod
    @abc.abstractmethod
    def translate_query(self, query):
        raise NotImplemented

    @classmethod
    @abc.abstractmethod
    def translate_sorting(self, sorting):
        raise NotImplemented

    @classmethod
    def translate_field(self, field):
        return field.to_document()
