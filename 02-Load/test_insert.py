import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "02-Load"))
from load import get_db_connection
from sqlalchemy import text

engine = get_db_connection()

try:
    with engine.begin() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM raw.orders"))
        count = result.scalar()
        print(f"Current order count: {count}")
        
        # Try inserting a test row
        conn.execute(text("""
            INSERT INTO raw.orders (id, created_at, total_price, currency, financial_status, order_number)
            VALUES (999999, '2026-05-14 00:00:00', 99.99, 'USD', 'paid', 9999)
        """))
        
        result = conn.execute(text("SELECT COUNT(*) FROM raw.orders"))
        count = result.scalar()
        print(f"After insert: {count}")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    engine.dispose()