import os
import sys
import time
import requests
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

# --- CONFIGURAZIONI ---
SCRYFALL_SETS_URL = "https://api.scryfall.com/sets"
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

HEADERS = {
    "User-Agent": "MtgArbitrageApp-SetsUpdater/1.1",
    "Accept": "application/json"
}

def get_scryfall_sets():
    """Scarica i metadati di tutti i set da Scryfall."""
    print("1. Download dei metadati dei set da Scryfall...")
    response = requests.get(SCRYFALL_SETS_URL, headers=HEADERS)
    response.raise_for_status()
    return response.json().get("data", [])

def download_svg_code(svg_url):
    """Scarica il codice sorgente (testo) del file SVG."""
    try:
        response = requests.get(svg_url, headers=HEADERS)
        response.raise_for_status()
        return response.text # Ritorna il codice <svg> puro
    except Exception as e:
        print(f"   Errore download SVG ({svg_url}): {e}")
        return None

def process_sets(sets_data):
    """Estrae i dati necessari e scarica i file SVG rispettando i rate limits."""
    print(f"2. Inizio elaborazione di {len(sets_data)} set. Questo richiederà un po' di tempo...")
    records = []
    
    for i, mtg_set in enumerate(sets_data):
        set_code = mtg_set.get("code")
        name = mtg_set.get("name")
        set_type = mtg_set.get("set_type")
        released_at = mtg_set.get("released_at")
        
        # Ci serve l'URI solo per il download, non per il salvataggio
        svg_uri = mtg_set.get("icon_svg_uri")
        
        # Ignora set privi di codice o di SVG
        if not set_code or not svg_uri:
            continue
            
        # Download del codice sorgente dell'SVG
        svg_raw_code = download_svg_code(svg_uri)
        
        # Gestione del null per la data (alcuni set futuri potrebbero non averla)
        if not released_at:
            released_at = None
            
        # Creazione della tupla SENZA l'URI di fallback
        records.append((
            set_code, 
            name, 
            set_type, 
            released_at, 
            svg_raw_code
        ))
        
        # Stampa il progresso ogni 50 set per non intasare i log
        if (i + 1) % 50 == 0:
            print(f"   Elaborati {i + 1}/{len(sets_data)} set...")
            
        # REGOLA FONDAMENTALE SCRYFALL: Pausa di 150 millisecondi (max 10 req/sec)
        time.sleep(0.15)
        
    print(f"   Elaborazione completata: {len(records)} set pronti per il database.")
    return records

def load_to_supabase(records):
    """Carica i dati su Supabase usando l'operazione di UPSERT."""
    if not DATABASE_URL:
        raise ValueError("CRITICAL ERROR: Variabile SUPABASE_DB_URL mancante!")

    print("3. Connessione a Supabase in corso...")
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 2, DATABASE_URL)
    conn = connection_pool.getconn()
    
    try:
        with conn.cursor() as cursor:
            # Query aggiornata senza la colonna icon_svg_uri
            upsert_query = """
                INSERT INTO public.sets (
                    set_code, name, set_type, released_at, icon_svg_raw
                )
                VALUES %s
                ON CONFLICT (set_code) 
                DO UPDATE SET 
                    name = EXCLUDED.name,
                    set_type = EXCLUDED.set_type,
                    released_at = EXCLUDED.released_at,
                    icon_svg_raw = EXCLUDED.icon_svg_raw;
            """
            print("4. Esecuzione del Bulk UPSERT...")
            execute_values(cursor, upsert_query, records, page_size=100)
            
        conn.commit()
        print("5. SUCCESS: Database dei set aggiornato correttamente!")
        
    except Exception as e:
        print(f"   ERRORE DB: {e}")
        conn.rollback()
        raise e
    finally:
        connection_pool.putconn(conn)
        connection_pool.closeall()

def main():
    try:
        sets_data = get_scryfall_sets()
        records = process_sets(sets_data)
        load_to_supabase(records)
    except Exception as e:
        print(f"Errore critico durante l'esecuzione: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
