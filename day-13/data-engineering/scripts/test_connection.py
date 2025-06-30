# scripts/test_connection.py
import psycopg2
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connection():
    """Test database connection and setup"""
    
    # Database configuration
    db_configs = [
        {
            'name': 'Docker PostgreSQL',
            'host': 'localhost',
            'port': 5432,
            'database': 'retail_dw',
            'user': 'dw_admin',
            'password': 'warehouse123'
        },
        {
            'name': 'Docker PostgreSQL (postgres user)',
            'host': 'localhost',
            'port': 5432,
            'database': 'postgres',
            'user': 'postgres',
            'password': 'postgres'
        }
    ]
    
    for config in db_configs:
        logger.info(f"\nTesting connection: {config['name']}")
        try:
            conn = psycopg2.connect(**{k: v for k, v in config.items() if k != 'name'})
            logger.info("✅ Connection successful!")
            
            with conn.cursor() as cursor:
                cursor.execute("SELECT current_user, current_database(), version();")
                user, db, version = cursor.fetchone()
                logger.info(f"   User: {user}")
                logger.info(f"   Database: {db}")
                logger.info(f"   Version: {version[:50]}...")
                
                # Try to list schemas
                cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast');")
                schemas = cursor.fetchall()
                logger.info(f"   Schemas: {[s[0] for s in schemas]}")
            
            conn.close()
            return config
            
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
    
    return None

def setup_database_if_needed(config):
    """Setup database if connection as postgres user works"""
    if config['user'] != 'postgres':
        return
        
    logger.info("Setting up database with postgres user...")
    try:
        conn = psycopg2.connect(**{k: v for k, v in config.items() if k != 'name'})
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            # Create database if it doesn't exist
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'retail_dw';")
            if not cursor.fetchone():
                cursor.execute("CREATE DATABASE retail_dw;")
                logger.info("Created retail_dw database")
            
            # Create user if it doesn't exist
            cursor.execute("SELECT 1 FROM pg_user WHERE usename = 'dw_admin';")
            if not cursor.fetchone():
                cursor.execute("CREATE USER dw_admin WITH PASSWORD 'warehouse123';")
                cursor.execute("GRANT ALL PRIVILEGES ON DATABASE retail_dw TO dw_admin;")
                logger.info("Created dw_admin user")
        
        conn.close()
        
        # Now try connecting as dw_admin
        logger.info("Testing dw_admin connection...")
        dw_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'retail_dw',
            'user': 'dw_admin',
            'password': 'warehouse123'
        }
        
        conn = psycopg2.connect(**dw_config)
        with conn.cursor() as cursor:
            # Create schemas
            schemas = ['staging', 'warehouse', 'mart']
            for schema in schemas:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.commit()
            logger.info("Created schemas: staging, warehouse, mart")
        
        conn.close()
        return dw_config
        
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return None

if __name__ == "__main__":
    logger.info("Starting database connection test...")
    
    working_config = test_connection()
    
    if working_config:
        logger.info(f"\n✅ Successfully connected with: {working_config['name']}")
        
        if working_config['user'] == 'postgres':
            final_config = setup_database_if_needed(working_config)
            if final_config:
                logger.info("✅ Database setup completed! Ready to run ETL.")
            else:
                logger.error("❌ Database setup failed.")
        else:
            logger.info("✅ dw_admin user already working! Ready to run ETL.")
    else:
        logger.error("❌ No working database connection found.")
        logger.info("\nTroubleshooting tips:")
        logger.info("1. Make sure Docker containers are running: docker ps")
        logger.info("2. Check container logs: docker logs retail-warehouse")
        logger.info("3. Try restarting containers: docker-compose down && docker-compose up -d")
