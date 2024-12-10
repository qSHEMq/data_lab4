import json
import sqlite3


# Функция для чтения обновлений из файла
def read_upd(path):
    updates = []
    with open(path, "r", encoding="utf-8") as f:
        update = {}
        for line in f:
            if line == "=====\n":
                updates.append(update)
                update = {}
                continue
            pair = line.strip().split("::")
            update[pair[0]] = pair[1]

        for update in updates:
            if update["method"] == "available":
                update["param"] = bool(update["param"])
            elif update["method"] != "remove":
                update["param"] = float(update["param"])

    return updates


# Создание таблицы товаров
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            price REAL,
            quantity INTEGER,
            category TEXT,
            fromCity TEXT,
            isAvailable BOOLEAN,
            views INTEGER,
            update_count INTEGER DEFAULT 0
        )
    """
    )
    conn.commit()


# Загрузка данных из JSON файла
def load_json_data(conn, file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
        cursor = conn.cursor()
        for item in data:
            cursor.execute(
                """
                INSERT INTO products (name, price, quantity, category, fromCity, isAvailable, views)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    item["name"],
                    item["price"],
                    item["quantity"],
                    item.get("category"),
                    item["fromCity"],
                    item["isAvailable"],
                    item["views"],
                ),
            )
        conn.commit()


# Обработка изменений из списка
def process_updates(conn, updates):
    for update in updates:
        name = update["name"]
        method = update["method"]
        param = update["param"]
        cursor = conn.cursor()

        # Начало транзакции
        conn.execute("BEGIN")
        try:
            if method == "update_price":
                cursor.execute(
                    """
                    UPDATE products
                    SET price = price + ?, update_count = update_count + 1
                    WHERE name = ? AND price + ? >= 0
                """,
                    (param, name, param),
                )
            elif method == "update_quantity":
                cursor.execute(
                    """
                    UPDATE products
                    SET quantity = quantity + ?, update_count = update_count + 1
                    WHERE name = ? AND quantity + ? >= 0
                """,
                    (param, name, param),
                )
            elif method == "remove":
                cursor.execute("DELETE FROM products WHERE name = ?", (name,))
            # Применение изменений
            conn.commit()
        except Exception as e:
            # Откат в случае ошибки
            conn.rollback()
            print(f"Error processing update for {name}: {e}")


# Анализ данных
def analyze_data(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT name, update_count FROM products
        ORDER BY update_count DESC
        LIMIT 10
    """
    )
    top_products = cursor.fetchall()
    print("Top 10 updated products:", top_products)

    cursor.execute(
        """
        SELECT category, SUM(price), MIN(price), MAX(price), AVG(price), COUNT(*)
        FROM products
        GROUP BY category
    """
    )
    price_analysis = cursor.fetchall()
    print("Price analysis:", price_analysis)

    cursor.execute(
        """
        SELECT category, SUM(quantity), MIN(quantity), MAX(quantity), AVG(quantity)
        FROM products
        GROUP BY category
    """
    )
    quantity_analysis = cursor.fetchall()
    print("Quantity analysis:", quantity_analysis)

    # Произвольный запрос
    cursor.execute(
        """
        SELECT name FROM products WHERE views > 20000
    """
    )
    popular_products = cursor.fetchall()
    print("Popular products with more than 20,000 views:", popular_products)


def main():
    conn = sqlite3.connect(":memory:")  # Для тестирования используем in-memory базу
    create_table(conn)
    load_json_data(conn, "data/4/_product_data.json")
    updates = read_upd("data/4/_update_data.msgpack")
    process_updates(conn, updates)
    analyze_data(conn)
    conn.close()


if __name__ == "__main__":
    main()
