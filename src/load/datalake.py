import os
from pathlib import Path
import json
import xml.dom.minidom

import pandas as pd


PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
BRONZE_DATA_DIR = os.path.join(DATA_DIR, 'bronze')
SILVER_DATA_DIR = os.path.join(DATA_DIR, 'silver')
GOLD_DATA_DIR = os.path.join(DATA_DIR, 'gold')
BRONZE_TIER_ALLOWED_EXTENSIONS = ['.xml', '.json']
SILVER_TIER_ALLOWED_EXTENSIONS = ['.parquet', '.json']


def save_to_bronze(content: str | bytes, filepath: str) -> None:
    full_path = os.path.join(BRONZE_DATA_DIR, filepath)
    os.makedirs(os.path.dirname(full_path), exist_ok = True)
    
    if not any(filepath.endswith(ext) for ext in BRONZE_TIER_ALLOWED_EXTENSIONS):
        raise ValueError(f"Formato de arquivo não suportado para bronze. Permitidos: {BRONZE_TIER_ALLOWED_EXTENSIONS}")

    with open(full_path, 'w', encoding='utf-8') as arquivo:
        if filepath.endswith('.json'):
            json.dump(content, arquivo, ensure_ascii = False, indent = 4)
        elif filepath.endswith('.xml'):
            dom = xml.dom.minidom.parseString(content)
            xml_formatado = dom.toprettyxml(indent="  ", encoding="utf-8").decode('utf-8')
            with open(full_path, 'w', encoding='utf-8') as arquivo:
                arquivo.write(xml_formatado)


def load_from_bronze(filepath: str) -> str | dict:
    full_path = os.path.join(BRONZE_DATA_DIR, filepath)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Arquivo não encontrado em bronze: {full_path}")
    
    if not any(filepath.endswith(ext) for ext in BRONZE_TIER_ALLOWED_EXTENSIONS):
        raise ValueError(f"Formato de arquivo não suportado para bronze. Permitidos: {BRONZE_TIER_ALLOWED_EXTENSIONS}")

    with open(full_path, 'r', encoding='utf-8') as arquivo:
        if filepath.endswith('.json'):
            return json.load(arquivo)
        elif filepath.endswith('.xml'):
            return arquivo.read()


def save_to_silver(content: pd.DataFrame | dict, filename: str) -> None:
    full_path = os.path.join(SILVER_DATA_DIR, filename)
    os.makedirs(os.path.dirname(full_path), exist_ok = True)

    if not any(filename.endswith(ext) for ext in SILVER_TIER_ALLOWED_EXTENSIONS):
        raise ValueError(f"Formato de arquivo não suportado para silver. Permitidos: {SILVER_TIER_ALLOWED_EXTENSIONS}")

    if filename.endswith('.json'):
        with open(full_path, 'w', encoding = 'utf-8') as arquivo:
            json.dump(content, arquivo, ensure_ascii = False, indent = 4)
    elif filename.endswith('.parquet'):
        if isinstance(content, pd.DataFrame):
            content.to_parquet(full_path, engine = 'pyarrow', index = False)
        else:
            raise ValueError("Para arquivos Parquet, o conteúdo deve ser um DataFrame do Pandas.")
