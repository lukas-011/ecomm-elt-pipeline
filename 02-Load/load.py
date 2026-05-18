import logging
import os
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

def get_db_connection():
    """Create PostgreSQL connection."""
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(db_url)
    return engine

def load_to_postgres(df, table_name, if_exists='append', chunksize=1000):
    """Load DataFrame to PostgreSQL using raw SQL."""
    if df.empty:
        logger.warning(f"DataFrame for {table_name} is empty, skipping.")
        return
    
    engine = get_db_connection()
    
    try:
        logger.info(f"Loading {len(df)} rows into {table_name} (chunksize={chunksize})...")
        
        with engine.begin() as conn:
            # Delete existing data if replace
            if if_exists == 'replace':
                conn.execute(text(f"DELETE FROM {table_name}"))
            
            # Insert in chunks
            for i in range(0, len(df), chunksize):
                chunk = df.iloc[i:i+chunksize]
                
                for _, row in chunk.iterrows():
                    columns = ', '.join(chunk.columns)
                    placeholders = ', '.join([f"'{str(val).replace(chr(39), chr(39)+chr(39))}'" if val is not None else 'NULL' for val in row])
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    conn.execute(text(sql))
        
        logger.info(f"Successfully loaded {len(df)} rows into {table_name}")
    except Exception as e:
        logger.error(f"Error loading to {table_name}: {e}", exc_info=True)
        raise
    finally:
        engine.dispose()

if __name__ == "__main__":
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            logger.info("PostgreSQL connection successful")
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise
    finally:
        engine.dispose()