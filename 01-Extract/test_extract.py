import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "01-Extract"))
from extract import get_access_token, extract_orders, extract_products

token = get_access_token()
store = "gym-whale-rxzfdcpx.myshopify.com"

orders = extract_orders(token, store)
products = extract_products(token, store)

print(f"Orders extracted: {len(orders)}")
print(f"Products extracted: {len(products)}")

if orders:
    print("First order:", orders[0])