# scripts/simple_connection_test.py
import psycopg2

def test_connection():
    db_config = {
        'host': '127.0.0.1',  # Use IPv4 explicitly
        'port': 5432,
        'database': 'retail_dw',
        'user': 'dw_admin',
        'password': 'warehouse123'
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        print("✅ Connection successful!")
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_user, current_database();")
            user, db = cursor.fetchone()
            print(f"User: {user}, Database: {db}")
            
            # Check schemas
            cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('staging', 'warehouse', 'mart');")
            schemas = [row[0] for row in cursor.fetchall()]
            print(f"Schemas: {schemas}")
            
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()
