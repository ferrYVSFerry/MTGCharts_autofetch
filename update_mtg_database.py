import requests
import json
import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
from datetime import datetime

# --- CONFIGURAZIONE COSTANTI ---
SCRYFALL_BULK_URL = "https://api.scryfall.com/bulk-data"
TEMP_BULK_FILE = "scryfall_bulk_raw.json"
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

HEADERS = {
    "User-Agent": "MtgArbitrageApp/1.5",
    "Accept": "application/json"
}

MAJOR_FORMATS = ["standard", "pioneer", "modern", "legacy", "vintage", "commander", "pauper"]
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")

def is_playable(card):
    """Verifica se la carta rispetta i criteri torneistici di base."""
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

def extract_essential_data(card):
    """Estrae solo i campi necessari."""
    legalities = card.get("legalities", {})
    legal_formats = [fmt for fmt in MAJOR_FORMATS if legalities.get(fmt) in ["legal", "restricted"]]
    
    prices = card.get("prices", {})
    price_eur = float(prices.get("eur") or prices.get("eur_foil") or 0) if (prices.get("eur") or prices.get("eur_foil")) else None
    price_usd = float(prices.get("usd") or prices.get("usd_foil") or 0) if (prices.get("usd") or prices.get("usd_foil")) else None
    
    return {
        "scryfall_id": card.get("id"),
        "name": card.get("name"),
        "set_code": card.get("set"),
        "set_type": card.get("set_type"),
        "price_eur": price_eur,
        "price_usd": price_usd,
        "legal_formats": legal_formats,
        "edhrec_rank": card.get("edhrec_rank")
    }

def main():
    if not DATABASE_URL:
        raise ValueError("ERRORE CRITICO: Variabile d'ambiente SUPABASE_DB_URL non trovata!")

    # 1. DOWNLOAD DA SCRYFALL
    print("1. Download dati da Scryfall...")
    response = requests.get(SCRYFALL_BULK_URL, headers=HEADERS)
    response.raise_for_status()
    download_uri = next((item.get("download_uri") for item in response.json().get("data", []) if item.get("type") == "default_cards"), None)
    
    with requests.get(download_uri, headers=HEADERS, stream=True) as r:
        r.raise_for_status()
        with open(TEMP_BULK_FILE, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                
    # 2. FILTRAGGIO
    print("2. Filtraggio dei dati...")
    records = []
    with open(TEMP_BULK_FILE, 'r', encoding='utf-8') as f:
        all_cards = json.load(f)
        for card in all_cards:
            if is_playable(card):
                data = extract_essential_data(card)
                # Salva solo se ha un prezzo USA valido
                if data["price_usd"] is not None:
                    records.append((
                        data["scryfall_id"], data["name"], data["set_code"], 
                        data["price_eur"], data["price_usd"], data["legal_formats"], 
                        data["edhrec_rank"], data["set_type"]
                    ))
    print(f"   Carte valide da aggiornare/inserire: {len(records)}")

    # 3. UPLOAD SU SUPABASE (UPSERT)
    print("3. Connessione a Supabase e avvio UPSERT...")
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 5, DATABASE_URL)
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cursor:
            upsert_query = """
                INSERT INTO public.cards (scryfall_id, name, set_code, price_eur, price_usd, legal_formats, edhrec_rank, set_type)
                VALUES %s
                ON CONFLICT (scryfall_id) 
                DO UPDATE SET 
                    price_eur = EXCLUDED.price_eur,
                    price_usd = EXCLUDED.price_usd,
                    legal_formats = EXCLUDED.legal_formats,
                    edhrec_rank = EXCLUDED.edhrec_rank,
                    set_type = EXCLUDED.set_type;
            """
            execute_values(cursor, upsert_query, records, page_size=1000)
        conn.commit()
        print("4. 🎉 Database aggiornato con successo!")
    except Exception as e:
        print(f"Errore DB: {e}")
        conn.rollback()
    finally:
        connection_pool.putconn(conn)
        connection_pool.closeall()
        if os.path.exists(TEMP_BULK_FILE):
            os.remove(TEMP_BULK_FILE)

if __name__ == "__main__":
    main()
