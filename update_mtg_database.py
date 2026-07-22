import os
import sys
import json
import math
import zipfile
import requests
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
from datetime import datetime

# --- CONFIGURATION CONSTANTS ---
SCRYFALL_BULK_URL = "https://api.scryfall.com/bulk-data"
MTGJSON_PRICES_URL = "https://mtgjson.com/api/v5/AllPricesToday.json.zip"
MTGJSON_IDENTIFIERS_URL = "https://mtgjson.com/api/v5/AllIdentifiers.json.zip"

DATABASE_URL = os.getenv("SUPABASE_DB_URL")

# File paths in GitHub Actions workspace
TEMP_SCRYFALL_FILE = "scryfall_raw.json"
TEMP_MTGJSON_PRICES_ZIP = "AllPricesToday.json.zip"
TEMP_MTGJSON_IDENTIFIERS_ZIP = "AllIdentifiers.json.zip"
TEMP_MTGJSON_PRICES = "AllPricesToday.json"
TEMP_MTGJSON_IDENTIFIERS = "AllIdentifiers.json"

HEADERS = {
    "User-Agent": "MtgArbitrageApp-GitHubActions/2.5",
    "Accept": "application/json"
}

MAJOR_FORMATS = [
    "standard", "pioneer", "modern", 
    "legacy", "vintage", "commander", "pauper"
]

CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")


def download_file(url, filepath, description):
    """Downloads a file from a URL with basic error handling."""
    print(f"-> Downloading: {description}...")
    response = requests.get(url, stream=True, headers=HEADERS)
    response.raise_for_status()
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    print(f"   Successfully saved: {filepath}")


def extract_zip(zip_path, extract_to="."):
    """Extracts a ZIP archive and deletes the archive file."""
    print(f"-> Extracting archive: {zip_path}...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    os.remove(zip_path)
    print("   Extraction completed and ZIP removed.")


def build_mtgjson_price_map():
    """Reads extracted MTGJSON files and maps TCGplayer IDs to median prices."""
    print("-> Building TCGplayer ID to Median Price lookup table...")
    
    with open(TEMP_MTGJSON_PRICES, 'r', encoding='utf-8') as f:
        prices_data = json.load(f).get("data", {})
        
    with open(TEMP_MTGJSON_IDENTIFIERS, 'r', encoding='utf-8') as f:
        identifiers_data = json.load(f).get("data", {})

    tcg_price_map = {}

    for uuid, id_info in identifiers_data.items():
        tcg_id = id_info.get("identifiers", {}).get("tcgplayerProductId")
        if not tcg_id:
            continue

        uuid_prices = prices_data.get(uuid, {})
        paper_prices = uuid_prices.get("paper", {}).get("tcgplayer", {})
        
        retail_normal = paper_prices.get("retail", {}).get("normal", {})
        if retail_normal:
            latest_date = max(retail_normal.keys())
            median_val = retail_normal[latest_date]
            if median_val is not None:
                tcg_price_map[str(tcg_id)] = float(median_val)

    print(f"   Successfully mapped {len(tcg_price_map)} prices from MTGJSON.")
    return tcg_price_map


def is_playable(card):
    """Checks if the card meets basic tournament and format criteria."""
    if card.get("released_at", "9999-12-31") > CURRENT_DATE:
        return False
    type_line = card.get("type_line", "")
    if "Basic Land" in type_line or "Basic Snow Land" in type_line:
        return False
    if card.get("name", "").startswith("A-"):
        return False
    if card.get("digital", True) == True:
        return False
    if "paper" not in card.get("games", []):
        return False
    
    legalities = card.get("legalities", {})
    if not any(legalities.get(fmt) in ["legal", "restricted"] for fmt in MAJOR_FORMATS):
        return False
        
    set_type = card.get("set_type", "")
    if set_type in ["funny", "memorabilia", "token", "alchemy"]:
        return False
    if card.get("oversized", False):
        return False
        
    return True


def calculate_arbitrage_score(price_eur, price_usd, edhrec_rank):
    """Calculates the Arbitrage Score based on the mathematical model."""
    if not price_eur or not price_usd or price_eur <= 0:
        return None

    rank = edhrec_rank if edhrec_rank is not None else 10000
    base_cost_eur = price_usd * 0.92
    
    # Apply import duties and VAT
    if base_cost_eur <= 150.00:
        landed_cost = base_cost_eur * 1.22  # 22% VAT
    else:
        landed_cost = base_cost_eur * 1.25  # 22% VAT + 3% Customs Duty

    try:
        numerator = price_eur - landed_cost
        denominator = math.sqrt(rank + 1) * math.log(price_eur + 1)
        score = numerator / denominator
        return round(score, 2)
    except ZeroDivisionError:
        return None


def transform_and_prepare_records(tcg_price_map):
    """Filters raw cards, merges MTGJSON median prices, and calculates scores."""
    print("-> Filtering cards and merging datasets...")
    records = []
    
    with open(TEMP_SCRYFALL_FILE, 'r', encoding='utf-8') as f:
        all_cards = json.load(f)
        
    for card in all_cards:
        if not is_playable(card):
            continue
            
        prices = card.get("prices", {})
        price_usd_raw = prices.get("usd") or prices.get("usd_foil")
        
        # Strict Rule: Must have a valid US market price
        if price_usd_raw is None:
            continue
            
        price_eur_raw = prices.get("eur") or prices.get("eur_foil")
        price_eur = float(price_eur_raw) if price_eur_raw is not None else None
        price_usd = float(price_usd_raw)
        
        # Lookup median price from MTGJSON mapping
        tcg_id = str(card.get("tcgplayer_id")) if card.get("tcgplayer_id") else None
        price_usd_median = tcg_price_map.get(tcg_id) if tcg_id else None
        
        legalities = card.get("legalities", {})
        legal_formats = [fmt for fmt in MAJOR_FORMATS if legalities.get(fmt) in ["legal", "restricted"]]
        edhrec_rank = card.get("edhrec_rank")
        
        # Calculate Arbitrage Score
        arbitrage_score = calculate_arbitrage_score(price_eur, price_usd, edhrec_rank)
        
        # Prepare PostgreSQL tuple
        records.append((
            card.get("id"),
            card.get("name"),
            card.get("set"),
            price_eur,
            price_usd,
            price_usd_median,
            legal_formats,
            edhrec_rank,
            card.get("set_type"),
            arbitrage_score
        ))
        
    print(f"   Valid cards ready for database UPSERT: {len(records)}")
    return records


def clean_up_temp_files():
    """Removes temporary raw files to free up runner disk space."""
    print("-> Cleaning up workspace temporary files...")
    for temp_file in [TEMP_SCRYFALL_FILE, TEMP_MTGJSON_PRICES, TEMP_MTGJSON_IDENTIFIERS]:
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"   Removed: {temp_file}")


def main():
    if not DATABASE_URL:
        raise ValueError("CRITICAL ERROR: SUPABASE_DB_URL environment variable is missing!")

    print("=== STARTING DAILY MTG DATABASE PIPELINE ===")
    
    # 1. FETCH SCRYFALL BULK DATA
    print("\n[Step 1/5] Fetching Scryfall metadata and dataset...")
    response = requests.get(SCRYFALL_BULK_URL, headers=HEADERS)
    response.raise_for_status()
    download_uri = next((item.get("download_uri") for item in response.json().get("data", []) if item.get("type") == "default_cards"), None)
    
    if not download_uri:
        raise Exception("Could not locate download URI for Scryfall Default Cards.")
    download_file(download_uri, TEMP_SCRYFALL_FILE, "Scryfall Bulk Data")

    # 2. FETCH AND EXTRACT MTGJSON DATA
    print("\n[Step 2/5] Fetching MTGJSON compressed datasets...")
    download_file(MTGJSON_PRICES_URL, TEMP_MTGJSON_PRICES_ZIP, "MTGJSON Prices (ZIP)")
    extract_zip(TEMP_MTGJSON_PRICES_ZIP)
    
    download_file(MTGJSON_IDENTIFIERS_URL, TEMP_MTGJSON_IDENTIFIERS_ZIP, "MTGJSON Identifiers (ZIP)")
    extract_zip(TEMP_MTGJSON_IDENTIFIERS_ZIP)

    # 3. TRANSFORM AND MERGE
    print("\n[Step 3/5] Processing, mapping, and calculating scores...")
    tcg_price_map = build_mtgjson_price_map()
    records = transform_and_prepare_records(tcg_price_map)

    # 4. DATABASE UPSERT
    print("\n[Step 4/5] Connecting to Supabase and executing bulk UPSERT...")
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 5, DATABASE_URL)
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cursor:
            upsert_query = """
                INSERT INTO public.cards (
                    scryfall_id, name, set_code, price_eur, price_usd, 
                    price_usd_median, legal_formats, edhrec_rank, set_type,
                    arbitrage_score
                )
                VALUES %s
                ON CONFLICT (scryfall_id) 
                DO UPDATE SET 
                    price_eur = EXCLUDED.price_eur,
                    price_usd = EXCLUDED.price_usd,
                    price_usd_median = EXCLUDED.price_usd_median,
                    legal_formats = EXCLUDED.legal_formats,
                    edhrec_rank = EXCLUDED.edhrec_rank,
                    set_type = EXCLUDED.set_type,
                    arbitrage_score = EXCLUDED.arbitrage_score;
            """
            execute_values(cursor, upsert_query, records, page_size=1000)
        conn.commit()
        print("   SUCCESS: Database synchronized successfully!")
    except Exception as e:
        print(f"   DATABASE ERROR: {e}")
        conn.rollback()
        raise e
    finally:
        connection_pool.putconn(conn)
        connection_pool.closeall()
        
    # 5. CLEANUP
    print("\n[Step 5/5] Cleaning up workspace...")
    clean_up_temp_files()
    
    print("\n=== PIPELINE COMPLETED SUCCESSFULLY ===")


if __name__ == "__main__":
    main()
