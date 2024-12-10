import pickle
import json
import sqlite3
import os

VAR = 100


def read_pickle_file(file_path):
    with open(file_path, "rb") as f:
        return pickle.load(f)


def read_text_file(file_path):
    records = []
    current_record = {}

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
        # Разделяем записи по =====, игнорируя пустые строки
        entries = [x.strip() for x in content.split("=====") if x.strip()]

        for entry in entries:
            record = {}
            for line in entry.strip().split("\n"):
                if "::" in line:
                    key, value = line.split("::", 1)
                    record[key.strip()] = value.strip()
            if record:
                records.append(record)

    return records


def create_database():
    conn = sqlite3.connect("task3/database.db")
    cursor = conn.cursor()

    # Сначала удалим таблицу, если она существует
    cursor.execute("DROP TABLE IF EXISTS songs")

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS songs (
        artist TEXT,
        song TEXT,
        duration_ms INTEGER,
        year INTEGER,
        tempo FLOAT,
        genre TEXT,
        instrumentalness FLOAT,
        acousticness FLOAT,
        energy FLOAT,
        explicit BOOLEAN,
        loudness FLOAT,
        popularity INTEGER
    )
    """
    )

    return conn, cursor


def insert_pickle_data(cursor, data):
    try:
        cursor.execute(
            """
        INSERT INTO songs (artist, song, duration_ms, year, tempo, genre, 
                          acousticness, energy, popularity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                str(data["artist"]),
                str(data["song"]),
                int(data["duration_ms"]),
                int(data["year"]),
                float(data["tempo"]),
                str(data["genre"]),
                float(data["acousticness"]),
                float(data["energy"]),
                int(data["popularity"]),
            ),
        )
    except Exception as e:
        print(f"Error inserting pickle data: {e}")
        print(f"Data: {data}")


def insert_text_data(cursor, data):
    for record in data:
        try:
            cursor.execute(
                """
            INSERT INTO songs (artist, song, duration_ms, year, tempo, genre,
                              instrumentalness, explicit, loudness)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    str(record["artist"]),
                    str(record["song"]),
                    int(record["duration_ms"]),
                    int(record["year"]),
                    float(record["tempo"]),
                    str(record["genre"]),
                    float(record.get("instrumentalness", 0)),
                    record.get("explicit", "False") == "True",
                    float(record.get("loudness", 0)),
                ),
            )
        except Exception as e:
            print(f"Error inserting text data: {e}")
            print(f"Record: {record}")


def query_and_save_to_json(cursor, query, filename, params=None):
    cursor.execute(query, params or ())
    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    with open(f"task3/{filename}", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


def main():
    # Создаем директорию task3, если её нет
    os.makedirs("task3", exist_ok=True)

    # Читаем данные
    pickle_data = read_pickle_file("data/3/_part_1.pkl")
    text_data = read_text_file("data/3/_part_2.text")

    # Создаем базу данных и таблицу
    conn, cursor = create_database()

    # Вставляем данные
    insert_pickle_data(cursor, pickle_data)
    insert_text_data(cursor, text_data)
    conn.commit()

    # Запрос 1: Вывод первых (VAR+10) отсортированных по duration_ms
    query_and_save_to_json(
        cursor,
        f"SELECT * FROM songs ORDER BY duration_ms DESC LIMIT {VAR+10}",
        "output1.json",
    )

    # Запрос 2: Статистика по tempo
    cursor.execute(
        """
    SELECT 
        SUM(tempo) as sum_tempo,
        MIN(tempo) as min_tempo,
        MAX(tempo) as max_tempo,
        AVG(tempo) as avg_tempo
    FROM songs
    """
    )
    stats = cursor.fetchone()
    print("\nСтатистика по tempo:")
    print(f"Сумма: {stats[0]:.2f}")
    print(f"Минимум: {stats[1]:.2f}")
    print(f"Максимум: {stats[2]:.2f}")
    print(f"Среднее: {stats[3]:.2f}")

    # Запрос 3: Частота жанров
    cursor.execute(
        """
    SELECT genre, COUNT(*) as count
    FROM songs
    GROUP BY genre
    ORDER BY count DESC
    """
    )
    print("\nЧастота жанров:")
    for genre, count in cursor.fetchall():
        print(f"{genre}: {count}")

    # Запрос 4: Фильтрация и сортировка
    query_and_save_to_json(
        cursor,
        f"""
        SELECT * FROM songs 
        WHERE duration_ms > 200000 
        ORDER BY tempo DESC 
        LIMIT {VAR+15}
        """,
        "output2.json",
    )

    conn.close()


if __name__ == "__main__":
    main()
