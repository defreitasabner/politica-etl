import xmltodict


class ProposicoesTransformer:
    
    def __init__(self):
        pass

    def xml_to_json(self, xml_proposicoes: str) -> list:
        return xmltodict.parse(xml_proposicoes)['proposicoes']['proposicao']