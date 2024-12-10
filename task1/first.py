import json
from common import load_pickle_data, get_sql_type, connect_database

VAR = 100
PICKLE_FILE = "data/1-2/item.pkl"
DATABASE_FILE = "task1/database.db"
OUTPUT_JSON_1 = "output1.json"
OUTPUT_JSON_4 = "output4.json"

FIELDS = [
    "id",
    "name",
    "street",
    "city",
    "zipcode",
    "floors",
    "year",
    "parking",
    "prob_price",
    "views",
]


def create_table(cursor, fields, data_sample):
    columns = {field: get_sql_type(data_sample.get(field)) for field in fields}
    columns_def = ", ".join(
        [f"{field} {datatype}" for field, datatype in columns.items()]
    )
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS items ({columns_def}, PRIMARY KEY(id));"
    )


def insert_data(cursor, fields, data):
    placeholders = ", ".join(["?"] * len(fields))
    fields_str = ", ".join(fields)
    insert_query = f"INSERT OR IGNORE INTO items ({fields_str}) VALUES ({placeholders})"

    data_to_insert = []
    for item in data:
        row = [
            (
                int(item.get(field))
                if isinstance(item.get(field), bool)
                else item.get(field)
            )
            for field in fields
        ]
        data_to_insert.append(tuple(row))

    cursor.executemany(insert_query, data_to_insert)


def execute_queries(cursor):
    # Запрос 1
    cursor.execute("SELECT * FROM items ORDER BY prob_price ASC LIMIT ?", (VAR + 10,))
    result1 = [
        dict(zip([col[0] for col in cursor.description], row))
        for row in cursor.fetchall()
    ]
    with open(OUTPUT_JSON_1, "w", encoding="utf-8") as f:
        json.dump(result1, f, ensure_ascii=False, indent=4)

    # Запрос 2
    cursor.execute(
        "SELECT SUM(prob_price) as sum, MIN(prob_price) as min, MAX(prob_price) as max, AVG(prob_price) as avg FROM items"
    )
    print(
        "Запрос 2:",
        dict(zip([col[0] for col in cursor.description], cursor.fetchone())),
    )

    # Запрос 3
    cursor.execute(
        "SELECT city, COUNT(*) as count FROM items GROUP BY city ORDER BY count DESC"
    )
    print("Запрос 3:", [dict(zip(["city", "count"], row)) for row in cursor.fetchall()])

    # Запрос 4 - находим среднюю цену для определения порога
    cursor.execute("SELECT AVG(prob_price) FROM items")
    avg_price = cursor.fetchone()[0]
    threshold = avg_price  # используем среднюю цену как порог

    cursor.execute(
        """
        SELECT * FROM items 
        WHERE prob_price > ? 
        ORDER BY prob_price ASC 
        LIMIT ?
    """,
        (threshold, VAR + 10),
    )

    result4 = [
        dict(zip([col[0] for col in cursor.description], row))
        for row in cursor.fetchall()
    ]

    # Проверяем, есть ли данные
    if not result4:
        print("Предупреждение: Нет данных, удовлетворяющих условию в запросе 4")
        # Используем более мягкое условие, если нет результатов
        cursor.execute(
            """
            SELECT * FROM items 
            ORDER BY prob_price DESC 
            LIMIT ?
        """,
            (VAR + 10,),
        )
        result4 = [
            dict(zip([col[0] for col in cursor.description], row))
            for row in cursor.fetchall()
        ]

    with open(OUTPUT_JSON_4, "w", encoding="utf-8") as f:
        json.dump(result4, f, ensure_ascii=False, indent=4)

    print(f"Запрос 4: записано {len(result4)} строк в {OUTPUT_JSON_4}")


def main():
    data = load_pickle_data(PICKLE_FILE)
    conn, cursor = connect_database(DATABASE_FILE)

    create_table(cursor, FIELDS, data[0])
    insert_data(cursor, FIELDS, data)
    execute_queries(cursor)

    conn.close()


if __name__ == "__main__":
    main()
