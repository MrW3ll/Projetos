from sqlalchemy import create_engine
from urllib.parse import quote_plus
from pathlib import Path
from dotenv import load_dotenv
from Python_arq.db_config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE

load_dotenv()

def get_engine():
    password_encoded = quote_plus(DB_PASSWORD)

    engine  = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{password_encoded}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"
    )
    return engine


def load_query(query_name):
    from pathlib import Path

    base_path = Path.cwd()

    # sobe até achar pasta sql
    while not (base_path / 'sql').exists():
        base_path = base_path.parent

    path = base_path / 'sql' / query_name

    with open(path, 'r', encoding='utf-8') as file:
        print(f'Carregando query: {query_name}')
        return file.read()


