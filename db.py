import os
import threading
from psycopg2 import pool

db_pool = None
db_pool_lock = threading.Lock()

def init_db_pool():
    global db_pool
    with db_pool_lock:
        if db_pool is None:
            try:
                db_pool = pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,
                    user=os.getenv('DB_USER'),
                    password=os.getenv('DB_PASSWORD'),
                    host=os.getenv('DB_HOST'),
                    port=os.getenv('DB_PORT'),
                    database=os.getenv('DB_NAME')
                )
                print("Database connection pool created")
            except Exception as e:
                print("Error creating connection pool:", e)
                raise

def get_db_connection():
    global db_pool
    if db_pool is None:
        init_db_pool()
    try:
        return db_pool.getconn()
    except Exception as e:
        print("Error getting DB connection:", e)
        raise

def put_db_connection(conn):
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)

def close_db_pool():
    global db_pool
    if db_pool:
        db_pool.closeall()
        print("All pooled connections closed")
        db_pool = None
