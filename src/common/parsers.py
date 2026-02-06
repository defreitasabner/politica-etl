import xmltodict


def xml_to_dict(xml_str: str) -> list:
    return xmltodict.parse(xml_str)

