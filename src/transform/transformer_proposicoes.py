import xmltodict
import pandas as pd

class TransformerProposicoes:
    
    def __init__(self):
        pass

    def xml_to_json(self, xml_proposicoes: str) -> list:
        return xmltodict.parse(xml_proposicoes)['proposicoes']['proposicao']
    
    def json_to_dataframe(self, json_proposicoes: list) -> str:
        return pd.json_normalize(json_proposicoes)