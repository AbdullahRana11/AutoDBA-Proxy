import mysql.connector
import threading
import time
import random

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'autodba_admin',
    'password': 'StrongPassword123!',
    'database': 'autodba_test',
    'port': 3306
}

NUM_THREADS = 15  # Number of concurrent "users" slamming the database
SLOW_QUERY_THRESHOLD_MS = 500  # Anything over half a second is flagged

def stress_test_worker(thread_id):
    """This function runs inside each thread, continuously hammering the DB."""
    try:
        # Crucial: Each thread MUST have its own dedicated database connection.
        # If they share one, MySQL will block them and throw errors.
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print(f"[+] Thread {thread_id} connected and attacking...")
        
        while True:
            # The intentionally terrible, unindexed JOIN query
            bad_query = """
                SELECT u.first_name, u.email, p.product_name, o.order_date, o.status
                FROM Orders o
                JOIN Users u ON o.user_id = u.user_id
                JOIN Products p ON o.product_id = p.product_id
                WHERE p.category = 'Electronics' 
                AND o.status = 'Pending'
                ORDER BY o.order_date DESC
                LIMIT 50;
            """
            
            # Start the timer
            start_time = time.time()
            
            cursor.execute(bad_query)
            results = cursor.fetchall() # Fetch the data to force the DB to do the work
            
            # Stop the timer and convert to milliseconds
            execution_time_ms = (time.time() - start_time) * 1000
            
            if execution_time_ms > SLOW_QUERY_THRESHOLD_MS:
                print(f"[WARN] Thread {thread_id}: Query took {execution_time_ms:.2f} ms!")
            else:
                print(f"[INFO] Thread {thread_id}: Query took {execution_time_ms:.2f} ms")
                
            # Sleep for a fraction of a second so we don't completely lock up the OS
            time.sleep(random.uniform(0.1, 0.5))
            
    except mysql.connector.Error as err:
        print(f"[-] Thread {thread_id} Error: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def main():
    print("[*] Starting the Database Chaos Generator...")
    print(f"[*] Spawning {NUM_THREADS} concurrent threads...\n")
    
    threads = []
    
    # Spin up the worker threads
    for i in range(NUM_THREADS):
        t = threading.Thread(target=stress_test_worker, args=(i,))
        t.daemon = True # This ensures threads die when you hit Ctrl+C
        t.start()
        threads.append(t)
        time.sleep(0.1) # Stagger the connections slightly
        
    try:
        # Keep the main thread alive while the workers do their job
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[*] Stopping the attack. Shutting down threads...")

if __name__ == "__main__":
    main()