import os
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(env_path)

def check_everything():
    # Lay thong tin tu .env
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    port = os.getenv("DB_PORT")
    host = "127.0.0.1"

    print("--- 1. Testing Raw Connection (psycopg2 via TCP/IP) ---")
    try:
        # Dung host=127.0.0.1 va sslmode=require
        conn = psycopg2.connect(
            host=host,
            database=db_name,
            user=user,
            password=password,
            port=port,
            sslmode='require'
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        print(f"Success: {cur.fetchone()[0]}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Raw connection FAILED: {e}")
        print("Kiem tra xem postgresql.conf da bat ssl = on chua.")
        return

    print("\n--- 2. Testing SQLAlchemy Connection (TCP/IP + SSL) ---")
    try:
        url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}?sslmode=require"
        
        engine = create_engine(url)
        
        with engine.connect() as connection:
            query = text("""
                SELECT ssl, version, cipher, bits 
                FROM pg_stat_ssl 
                WHERE pid = pg_backend_pid();
            """)
            result = connection.execute(query).fetchone()
            
            print("SQLAlchemy connection: OK")
            if result and result[0]:
                print("SSL STATUS: ACTIVE")
                print(f"TLS Version: {result[1]}")
                print(f"Cipher: {result[2]}")
            else:
                print("SSL STATUS: INACTIVE")
                
    except Exception as e:
        print(f"SQLAlchemy connection FAILED: {e}")

if __name__ == "__main__":
    check_everything()
