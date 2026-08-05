"""
MTG Database Synchronization Pipeline
=====================================
This script automates the daily extraction, transformation, and loading (ETL) 
of Magic: The Gathering card data into a Supabase PostgreSQL instance.

Core Workflow:
1. Fetches the live USD to EUR exchange rate via a public API.
2. Retrieves the daily bulk card dataset from Scryfall (handles both JSON and JSONL formats).
3. Downloads and extracts daily pricing and product identifier data from MTGJSON.
4. Constructs a localized hash map linking TCGplayer IDs to median retail prices.
5. Processes Scryfall records: filters non-playable entities, merges pricing data, 
   and calculates custom Arbitrage Scores, ROI ratios, and landed costs.
6. Executes a bulk UPSERT into the Supabase database. Utilizes progressive chunking 
   and strict type casting to prevent PostgreSQL memory commitment saturation (OOM risk) 
   and ensure type safety for UUIDs and arrays.

Environment Variables:
- SUPABASE_DB_URL: PostgreSQL connection string.
"""

import os
import sys
import json
import math
import zipfile
import requests
import psycopg2
import gzip
from psycopg2 import pool
from psycopg2.extras import execute_values
from datetime import datetime

# --- CONFIGURATION CONSTANTS ---
SCRYFALL_BULK_URL = "https://api.scryfall.com/bulk-data/default_cards"
MTGJSON_PRICES_URL = "https://mtgjson.com/api/v5/AllPricesToday.json.zip"
MTGJSON_IDENTIFIERS_URL = "https://mtgjson.com/api/v5/AllIdentifiers.json.zip"
EXCHANGE_RATE_API_URL = "https://api.frankfurter.app/latest?from=USD&to=EUR"

DATABASE_URL = os.getenv("SUPABASE_DB_URL")

# Temporary file paths in GitHub Actions workspace
TEMP_SCRYFALL_FILE = "scryfall_raw.dat"
TEMP_MTGJSON_PRICES_ZIP = "AllPricesToday.json.zip"
TEMP_MTGJSON_IDENTIFIERS_ZIP = "AllIdentifiers.json.zip"
TEMP_MTGJSON_PRICES = "AllPricesToday.json"
TEMP_MTGJSON_IDENTIFIERS = "AllIdentifiers.json"

HEADERS = {
    "User-Agent": "MtgArbitrageApp-GitHubActions/3.4 (info@mtgarbitrage.com)",
    "Accept": "application/json"
}

MAJOR_FORMATS = [
    "standard", "pioneer", "modern", 
    "legacy", "vintage", "commander", "pauper"
]

CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")


def get_usd_to_eur_rate():
    """Fetches the live USD to EUR exchange rate using a free API."""
    print("-> Fetching live USD to EUR exchange rate...")
    try:
        response = requests.get(EXCHANGE_RATE_API_URL, timeout=10)
        response.raise_for_status()
        rate = response.json().get('rates', {}).get('EUR')
        if rate:
            print(f"   Live rate fetched: 1 USD = {rate} EUR")
            return float(rate)
        else:
            raise ValueError("Rate not found in API response payload.")
    except Exception as e:
        print(f"   WARNING: Exchange rate fetch failed ({e}). Falling back to 0.92 default.")
        return 0.92


def download_file(url, filepath, description):
    """Downloads a file from a remote endpoint using streaming to optimize memory allocation."""
    print(f"-> Downloading: {description}...")
    response = requests.get(url, stream=True, headers=HEADERS)
    response.raise_for_status()
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    print(f"   Successfully saved payload to: {filepath}")


def extract_zip(zip_path, extract_to="."):
    """Extracts a ZIP archive into the specified directory and purges the raw archive to free disk space."""
    print(f"-> Extracting archive: {zip_path}...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    os.remove(zip_path)
    print("   Extraction completed and source ZIP removed.")


def build_mtgjson_price_map():
    """
    Parses extracted MTGJSON payloads and builds an O(1) lookup dictionary.
    Maps TCGplayer Product IDs to their latest median normal paper prices.
    """
    print("-> Building TCGplayer ID to Median Price lookup map...")
    
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

    print(f"   Successfully mapped {len(tcg_price_map)} distinct price nodes from MTGJSON.")
    return tcg_price_map


def is_playable(card):
    """
    Validates if a card entity meets baseline tournament and format criteria.
    Filters out basic lands, digital-only, oversized, and memorabilia set subsets.
    """
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


def calculate_arbitrage_score(price_eur, price_usd, edhrec_rank, exchange_rate):
    """
    Computes financial metrics including the Arbitrage Score, Estimated Net Profit, 
    and Return on Investment (ROI) ratio, applying simulated import duties and VAT based on thresholds.
    """
    if not price_eur or not price_usd or price_eur <= 0 or price_usd <= 0:
        return None, None, None

    rank = edhrec_rank if edhrec_rank is not None else 10000
    
    base_cost_eur = price_usd * exchange_rate
    
    # Apply standard EU tax models (22% VAT, +3% Customs for > 150 EUR)
    if base_cost_eur <= 150.00:
        landed_cost = base_cost_eur * 1.22
    else:
        landed_cost = base_cost_eur * 1.25

    estimated_profit = price_eur - landed_cost
    
    profit_ratio = round(estimated_profit / landed_cost, 4) if landed_cost > 0 else None

    try:
        # Weighted logarithmic algorithm to score arbitrage viability against market demand
        denominator = math.sqrt(rank + 1) * math.log(price_eur + 1)
        score = estimated_profit / denominator
        return round(score, 2), round(estimated_profit, 2), profit_ratio
    except ZeroDivisionError:
        return None, round(estimated_profit, 2), profit_ratio


def process_single_card(card, tcg_price_map, exchange_rate):
    """
    Extracts, normalizes, and packages relevant data attributes from a single raw Scryfall payload 
    into a structured tuple formatted for PostgreSQL bulk insertion.
    """
    if not is_playable(card):
        return None
        
    prices = card.get("prices", {})
    price_usd_raw = prices.get("usd") or prices.get("usd_foil")
    
    if price_usd_raw is None:
        return None
        
    price_eur_raw = prices.get("eur") or prices.get("eur_foil")
    price_eur = float(price_eur_raw) if price_eur_raw is not None else None
    price_usd = float(price_usd_raw)
    
    tcg_id = str(card.get("tcgplayer_id")) if card.get("tcgplayer_id") else None
    price_usd_median = tcg_price_map.get(tcg_id) if tcg_id else None
    
    legalities = card.get("legalities", {})
    legal_formats = [fmt for fmt in MAJOR_FORMATS if legalities.get(fmt) in ["legal", "restricted"]]
    edhrec_rank = card.get("edhrec_rank")
    
    arbitrage_score, estimated_profit_eur, profit_ratio = calculate_arbitrage_score(price_eur, price_usd, edhrec_rank, exchange_rate)
    
    # Extract the ontological type line for granular card categorization
    card_type = card.get("type_line", "")
    
    return (
        card.get("id"),
        card.get("name"),
        card.get("set"),
        price_eur,
        price_usd,
        price_usd_median,
        legal_formats,
        edhrec_rank,
        card.get("set_type"),
        arbitrage_score,
        estimated_profit_eur,
        profit_ratio,
        card_type
    )


def transform_and_prepare_records(tcg_price_map, exchange_rate):
    """
    Iterates over the Scryfall dataset, dynamically routing between JSON Array and JSONL parsing algorithms.
    Aggregates valid tuples into a master dataset array.
    """
    print("-> Filtering entities and performing dataset joins...")
    records = []
    
    # Identify binary compression signature for GZIP
    with open(TEMP_SCRYFALL_FILE, 'rb') as test_f:
        magic_number = test_f.read(2)
    is_gzipped = (magic_number == b'\x1f\x8b')
    
    open_func = gzip.open if is_gzipped else open
    
    with open_func(TEMP_SCRYFALL_FILE, 'rt', encoding='utf-8') as f:
        first_char = f.read(1)
        f.seek(0)
        
        if first_char == '[':
            print("   Format detected: Standard JSON Array")
            all_cards = json.load(f)
            for card in all_cards:
                record = process_single_card(card, tcg_price_map, exchange_rate)
                if record:
                    records.append(record)
        else:
            print("   Format detected: JSON Lines (JSONL)")
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    card = json.loads(line)
                    record = process_single_card(card, tcg_price_map, exchange_rate)
                    if record:
                        records.append(record)
                except json.JSONDecodeError:
                    continue
                    
    print(f"   Compiled validated tuples for UPSERT sequence: {len(records)}")
    return records


def clean_up_temp_files():
    """Purges intermediate temporary artifacts to avoid IO bloat on the runner instance."""
    print("-> Executing workspace cleanup...")
    for temp_file in [TEMP_SCRYFALL_FILE, TEMP_MTGJSON_PRICES, TEMP_MTGJSON_IDENTIFIERS]:
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"   Purged file artifact: {temp_file}")


def main():
    if not DATABASE_URL:
        raise ValueError("CRITICAL ERROR: 'SUPABASE_DB_URL' environment variable is unassigned.")

    print("=== STARTING DAILY MTG DATABASE PIPELINE ===")
    
    exchange_rate = get_usd_to_eur_rate()
    
    print("\n[Step 1/5] Fetching Scryfall metadata and executing dataset download...")
    response = requests.get(SCRYFALL_BULK_URL, headers=HEADERS)
    response.raise_for_status()
    
    response_data = response.json()
    download_uri = response_data.get("jsonl_download_uri") or response_data.get("download_uri")
    
    if not download_uri:
        raise Exception("Failed to resolve a valid data URI (JSON/JSONL) for Scryfall Default Cards endpoint.")
        
    download_file(download_uri, TEMP_SCRYFALL_FILE, f"Scryfall Bulk Data ({'GZIP JSONL' if 'jsonl' in download_uri else 'JSON'})")

    print("\n[Step 2/5] Fetching MTGJSON compressed dimensional datasets...")
    download_file(MTGJSON_PRICES_URL, TEMP_MTGJSON_PRICES_ZIP, "MTGJSON Prices (ZIP)")
    extract_zip(TEMP_MTGJSON_PRICES_ZIP)
    
    download_file(MTGJSON_IDENTIFIERS_URL, TEMP_MTGJSON_IDENTIFIERS_ZIP, "MTGJSON Identifiers (ZIP)")
    extract_zip(TEMP_MTGJSON_IDENTIFIERS_ZIP)

    print("\n[Step 3/5] Processing payloads, mapping identifiers, and calculating heuristic scores...")
    tcg_price_map = build_mtgjson_price_map()
    records = transform_and_prepare_records(tcg_price_map, exchange_rate)

    print(f"\n[Step 4/5] Establishing connection pool to Supabase and executing bulk UPSERT ({len(records)} records)...")
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 5, DATABASE_URL)
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cursor:
            upsert_query = """
                INSERT INTO public.cards (
                    scryfall_id, name, set_code, price_eur, price_usd, 
                    price_usd_median, legal_formats, edhrec_rank, set_type,
                    arbitrage_score, estimated_profit_eur, profit_ratio, card_type
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
                    arbitrage_score = EXCLUDED.arbitrage_score,
                    estimated_profit_eur = EXCLUDED.estimated_profit_eur,
                    profit_ratio = EXCLUDED.profit_ratio,
                    card_type = EXCLUDED.card_type;
            """
            
            # --- STRICT DATA TYPE CASTING TEMPLATE ---
            # Enforces explicit runtime casting for UUIDs and text arrays within the target database.
            # Mitigates native psycopg2 type inference mismatches against strict PostgreSQL schemas.
            val_template = "(%s::uuid, %s, %s, %s, %s, %s, %s::text[], %s, %s, %s, %s, %s, %s)"
            
            # --- PROGRESSIVE CHUNKING & TRANSACTION MANAGEMENT ---
            # Segregates the bulk payload into discrete batches to bypass Supabase Memory Commitment limits.
            CHUNK_SIZE = 5000 
            
            for i in range(0, len(records), CHUNK_SIZE):
                chunk = records[i:i + CHUNK_SIZE]
                
                # A suppressed page_size reduces the dimensions of the Abstract Syntax Tree (AST) 
                # generated by the PostgreSQL parser, throttling overhead spikes on work_mem.
                execute_values(
                    cursor, 
                    upsert_query, 
                    chunk, 
                    template=val_template, 
                    page_size=200
                )
                
                # Commit strictly at the chunk level to flush database transaction buffers 
                # and release kernel memory allocations progressively.
                conn.commit() 
                print(f"   Batch successfully committed: {len(chunk)} records (Total synchronized: {min(i + CHUNK_SIZE, len(records))}/{len(records)})")
                
        print("   SUCCESS: Target database synchronized with 0 errors.")
    except Exception as e:
        print(f"   CRITICAL DATABASE EXCEPTION: {e}")
        conn.rollback()
        raise e
    finally:
        connection_pool.putconn(conn)
        connection_pool.closeall()
        
    print("\n[Step 5/5] Dismantling temporary workspace artifacts...")
    clean_up_temp_files()
    
    print("\n=== ETL PIPELINE COMPLETED SUCCESSFULLY ===")


if __name__ == "__main__":
    main()
