import pickle
import msgpack
import sqlite3
from typing import List, Dict, Any


def get_sql_type(value: Any) -> str:
    if isinstance(value, bool):
        return "INTEGER"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    return "TEXT"


def load_pickle_data(file_path: str) -> List[Dict]:
    with open(file_path, "rb") as file:
        return pickle.load(file)


def load_msgpack_data(file_path: str) -> List[Dict]:
    with open(file_path, "rb") as file:
        return msgpack.unpack(file, raw=False)


def connect_database(db_path: str) -> tuple:
    conn = sqlite3.connect(db_path)
    return conn, conn.cursor()
