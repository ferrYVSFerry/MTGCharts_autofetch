import ijson
import json
import os
import asyncio
import traceback
import libsql_client
from datetime import datetime

# Source files downloaded by GitHub Actions
INPUT_FILE = "AllIdentifiers.json"
SET_FILE = "SetList.json"

DISCARDED_LAYOUTS = [
    "token", "double_faced_token", "emblem", "art_series", "vanguard", "planar", "scheme"
]
BATCH_SIZE = 500

async def weekly_update():
    print("[START] Starting weekly catalog update routine...")
    
    # 1. Retrieve Credentials from GitHub Secrets
    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")

    if not url or not token:
        print("[ERROR] Turso credentials not found in environment variables!")
        return

    # 2. Read Scheduled Sets to exclude future spoilers
    today = datetime.now().strftime('%Y-%m-%d')
    future_sets = {}
    
    if not os.path.exists(SET_FILE) or not os.path.exists(INPUT_FILE):
        print("[ERROR] JSON files missing. Ensure wget downloaded them.")
        return

    print("Reading scheduled expansions...")
    with open(SET_FILE, 'r', encoding='utf-8') as sf:
        set_data = json.load(sf)
        for expansion in set_data.get("data", []):
            release_date = expansion.get("releaseDate")
            set_code = expansion.get("code")
            if release_date and release_date > today:
                future_sets[set_code] = True

    # 3. Extract and Filter Cards
    print("Filtering and preparing valid cards...")
    cards_to_send = []
    
    with open(INPUT_FILE, 'rb') as f:
        items = ijson.kvitems(f, 'data')
        
        for card_uuid, card_data in items:
            set_code = card_data.get("setCode")
            
            # Application of all discussed filters
            if set_code in future_sets: continue
            if "Basic" in card_data.get("supertypes", []): continue
            if card_data.get("isFunny", False): continue
            if card_data.get("layout", "") in DISCARDED_LAYOUTS: continue
            if card_data.get("isAlternative", False): continue
            if card_data.get("number", "").startswith("A-"): continue

            # Prepare the validated card
            identifiers = card_data.get("identifiers", {})
            cards_to_send.append({
                "uuid": card_uuid,
                "name": card_data.get("name"),
                "set_code": set_code,
                "number": card_data.get("number"),
                "scryfall_id": identifiers.get("scryfallId"),
                "cardmarket_id": identifiers.get("cardmarketProductId"),
                "edhrec_rank": card_data.get("edhrecRank"),
                "track_price": 0
            })

    total_cards = len(cards_to_send)
    print(f"Found {total_cards:,} total legal cards. Connecting to Turso...")

    # 4. Send to Database (OPTIMIZED)
    sql = """
    INSERT OR IGNORE INTO cards (uuid, name, set_code, number, scryfall_id, cardmarket_id, edhrec_rank, track_price)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    try:
        async with libsql_client.create_client(url, auth_token=token) as client:
            for i in range(0, total_cards, BATCH_SIZE):
                batch = cards_to_send[i : i + BATCH_SIZE]
                statements = []
                
                for c in batch:
                    args = [
                        c["uuid"], c["name"], c["set_code"], c["number"],
                        c["scryfall_id"], c["cardmarket_id"], c["edhrec_rank"], c["track_price"]
                    ]
                    statements.append(libsql_client.Statement(sql, args))
                
                await client.batch(statements)
                
        print("Weekly update completed successfully!")
        print("New releases (if any) have been added. Excluded sets and Alchemy cards were ignored.")
        
    except Exception as e:
        print(f"[DATABASE ERROR] Unable to complete insertion: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(weekly_update())
