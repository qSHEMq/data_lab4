import json
from common import *

VAR = 100
DATABASE_FILE = "task1/database.db"
SUBITEM_FILE = "data/1-2/subitem.msgpack"
SUBITEM_FIELDS = [
    "name",
    "rating",
    "convenience",
    "security",
    "functionality",
    "comment",
]


def create_subitems_table(cursor):
    sample_entry = {
        "name": "",
        "rating": 0.0,
        "convenience": 0,
        "security": 0,
        "functionality": 0,
        "comment": "",
    }
    columns = {field: get_sql_type(sample_entry[field]) for field in SUBITEM_FIELDS}
    columns_def = ", ".join(
        [f"{field} {datatype}" for field, datatype in columns.items()]
    )
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS subitems ({columns_def}, FOREIGN KEY(name) REFERENCES items(name));"
    )


def insert_subitems(cursor, data):
    placeholders = ", ".join(["?"] * len(SUBITEM_FIELDS))
    fields_str = ", ".join(SUBITEM_FIELDS)
    cursor.executemany(
        f"INSERT INTO subitems ({fields_str}) VALUES ({placeholders})",
        [tuple(item.get(field) for field in SUBITEM_FIELDS) for item in data],
    )


def execute_relationship_queries(cursor):
    # Запрос 1: Полная информация о товарах с рейтингами
    cursor.execute(
        """
        SELECT i.id, i.name, i.city, i.prob_price,
               s.rating, s.convenience, s.security
        FROM items i
        JOIN subitems s ON i.name = s.name
        ORDER BY s.rating DESC
        LIMIT ?
    """,
        (VAR,),
    )
    result1 = [
        dict(zip([col[0] for col in cursor.description], row))
        for row in cursor.fetchall()
    ]
    with open("output_relationship_1.json", "w", encoding="utf-8") as f:
        json.dump(result1, f, ensure_ascii=False, indent=4)

    # Запрос 2: Аналитика по городам
    cursor.execute(
        """
        SELECT 
            i.city,
            COUNT(*) as total_items,
            AVG(s.rating) as avg_rating,
            AVG(i.prob_price) as avg_price,
            AVG(s.security) as avg_security
        FROM items i
        JOIN subitems s ON i.name = s.name
        GROUP BY i.city
        ORDER BY avg_rating DESC
    """
    )
    result2 = [
        dict(zip([col[0] for col in cursor.description], row))
        for row in cursor.fetchall()
    ]
    with open("output_relationship_2.json", "w", encoding="utf-8") as f:
        json.dump(result2, f, ensure_ascii=False, indent=4)

    # Запрос 3: Топ объектов по комплексной оценке
    cursor.execute(
        """
        SELECT 
            i.name,
            i.city,
            i.prob_price,
            s.rating,
            (s.convenience + s.security + s.functionality) as total_score
        FROM items i
        JOIN subitems s ON i.name = s.name
        WHERE i.prob_price > (SELECT AVG(prob_price) FROM items)
        AND s.rating > 4
        ORDER BY total_score DESC
        LIMIT ?
    """,
        (VAR,),
    )
    result3 = [
        dict(zip([col[0] for col in cursor.description], row))
        for row in cursor.fetchall()
    ]
    with open("output_relationship_3.json", "w", encoding="utf-8") as f:
        json.dump(result3, f, ensure_ascii=False, indent=4)


def main():
    subitems_data = load_msgpack_data(SUBITEM_FILE)
    conn, cursor = connect_database(DATABASE_FILE)

    create_subitems_table(cursor)
    insert_subitems(cursor, subitems_data)
    execute_relationship_queries(cursor)

    conn.close()


if __name__ == "__main__":
    main()
