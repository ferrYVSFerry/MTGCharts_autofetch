import ijson
import os
import asyncio
import traceback
import libsql_client
from datetime import datetime, timedelta

PRICES_FILE = "AllPrices.json"
BATCH_SIZE = 500

async def update_prices_and_cleanup():
    print("[START] Starting daily price update and retention cleanup routine...")
    
    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")

    if not url or not token:
        print("[ERROR] Turso credentials not found in environment variables.")
        return

    if not os.path.exists(PRICES_FILE):
        print(f"[ERROR] File '{PRICES_FILE}' not found in the current directory.")
        return

    today = datetime.now().strftime('%Y-%m-%d')
    
    # Calculate the retention threshold date (90 days ago)
    retention_days = 90
    cleanup_threshold_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d')
    
    records_to_insert = []

    try:
        async with libsql_client.create_client(url, auth_token=token) as client:
            
            # Step 1: Efficiently purge records older than 90 days
            print(f"Purging price history records older than {retention_days} days (before {cleanup_threshold_date})...")
            cleanup_sql = "DELETE FROM price_history WHERE date < ?"
            cleanup_result = await client.execute(cleanup_sql, [cleanup_threshold_date])
            print("Database cleanup completed successfully.")

            # Step 2: Fetch the UUIDs of the cards currently being tracked
            print("Fetching tracked cards from the database...")
            db_result = await client.execute("SELECT uuid FROM cards WHERE track_price = 1")
            
            tracked_uuids = {row[0] for row in db_result.rows}
            print(f"Found {len(tracked_uuids)} cards actively tracked.")

            if not tracked_uuids:
                print("No cards are currently set to be tracked. Skipping price updates.")
                return

            # Step 3: Parse the price data from AllPrices.json streamingly
            print(f"Parsing price data from {PRICES_FILE}...")
            with open(PRICES_FILE, 'rb') as f:
                items = ijson.kvitems(f, 'data')
                
                for card_uuid, price_data in items:
                    if card_uuid not in tracked_uuids:
                        continue
                    
                    paper = price_data.get("paper", {})
                    cardmarket = paper.get("cardmarket", {})
                    retail = cardmarket.get("retail", {})
                    
                    normal_prices = retail.get("normal", {})
                    foil_prices = retail.get("foil", {})
                    
                    # Try to fetch today's price, fallback to the latest available entry if not ready
                    price_normal = normal_prices.get(today)
                    if price_normal is None and normal_prices:
                        latest_date_normal = max(normal_prices.keys())
                        price_normal = normal_prices[latest_date_normal]
                        
                    price_foil = foil_prices.get(today)
                    if price_foil is None and foil_prices:
                        latest_date_foil = max(foil_prices.keys())
                        price_foil = foil_prices[latest_date_foil]
                        
                    if price_normal is not None or price_foil is not None:
                        records_to_insert.append((card_uuid, today, price_normal, price_foil))

            total_records = len(records_to_insert)
            print(f"Prepared {total_records} fresh price records for upload.")

            # Step 4: Batch insert today's prices using an upsert strategy
            insert_sql = """
            INSERT INTO price_history (card_uuid, date, price_normal, price_foil)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(card_uuid, date) DO UPDATE SET
                price_normal=excluded.price_normal,
                price_foil=excluded.price_foil;
            """

            for i in range(0, total_records, BATCH_SIZE):
                batch = records_to_insert[i : i + BATCH_SIZE]
                statements = [libsql_client.Statement(insert_sql, list(record)) for record in batch]
                await client.batch(statements)
                
            print("Daily price update and retention cleanup completed successfully.")

    except Exception as e:
        print(f"[FATAL ERROR] An issue occurred during execution: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(update_prices_and_cleanup())
