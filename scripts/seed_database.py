import mysql.connector
from faker import Faker
import random
import time

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'autodba_admin',
    'password': 'StrongPassword123!',
    'database': 'autodba_test',
    'port': 3306
}

NUM_USERS = 10000
NUM_PRODUCTS = 1000
NUM_ORDERS = 500000
BATCH_SIZE = 5000

fake = Faker()

def connect_to_db():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as err:
        print(f"[-] Database Connection Error: {err}")
        exit(1)

def seed_users(cursor):
    print("\n[*] Seeding Users...")
    query = "INSERT INTO Users (first_name, last_name, email, registration_date) VALUES (%s, %s, %s, %s)"
    
    for i in range(0, NUM_USERS, BATCH_SIZE):
        batch = []
        for _ in range(min(BATCH_SIZE, NUM_USERS - i)):
            batch.append((
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.date_time_between(start_date='-5y', end_date='now')
            ))
        cursor.executemany(query, batch)
        print(f"    -> Inserted {i + len(batch)} / {NUM_USERS} users")

def seed_products(cursor):
    print("\n[*] Seeding Products...")
    query = "INSERT INTO Products (product_name, category, price) VALUES (%s, %s, %s)"
    categories = ['Electronics', 'Clothing', 'Home', 'Toys', 'Sports', 'Books']
    
    for i in range(0, NUM_PRODUCTS, BATCH_SIZE):
        batch = []
        for _ in range(min(BATCH_SIZE, NUM_PRODUCTS - i)):
            batch.append((
                fake.word().capitalize() + " " + fake.word().capitalize(),
                random.choice(categories),
                round(random.uniform(5.00, 1500.00), 2)
            ))
        cursor.executemany(query, batch)
        print(f"    -> Inserted {i + len(batch)} / {NUM_PRODUCTS} products")

def seed_orders(cursor):
    print("\n[*] Seeding Orders (This will take a moment)...")
    query = "INSERT INTO Orders (user_id, product_id, order_date, status) VALUES (%s, %s, %s, %s)"
    statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
    
    for i in range(0, NUM_ORDERS, BATCH_SIZE):
        batch = []
        for _ in range(min(BATCH_SIZE, NUM_ORDERS - i)):
            batch.append((
                random.randint(1, NUM_USERS),     # Assumes user IDs are 1 to NUM_USERS
                random.randint(1, NUM_PRODUCTS),  # Assumes product IDs are 1 to NUM_PRODUCTS
                fake.date_time_between(start_date='-1y', end_date='now'),
                random.choice(statuses)
            ))
        cursor.executemany(query, batch)
        print(f"    -> Inserted {i + len(batch)} / {NUM_ORDERS} orders")

def main():
    start_time = time.time()
    conn = connect_to_db()
    cursor = conn.cursor()

    try:
        seed_users(cursor)
        conn.commit()
        
        seed_products(cursor)
        conn.commit()
        
        seed_orders(cursor)
        conn.commit()
        
        print("\n[+] Database seeded successfully!")
    except mysql.connector.Error as err:
        print(f"\n[-] Error during insertion: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        
    elapsed_time = round(time.time() - start_time, 2)
    print(f"[*] Total execution time: {elapsed_time} seconds")

if __name__ == "__main__":
    main()