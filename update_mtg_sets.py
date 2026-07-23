import os
import sys
import time
import requests
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

# --- CONFIGURAZIONI ---
SCRYFALL_SETS_URL = "https://api.scryfall.com/sets"

# Legge la variabile d'ambiente (GitHub Actions) oppure usa la stringa locale se presente
DATABASE_URL = os.getenv("SUPABASE_DB_URL", "postgresql://postgres.tuo_progetto:tua_password@aws-0-eu-central-1.pooler.supabase.com:6543/postgres")

HEADERS = {
    "User-Agent": "MtgArbitrageApp-SetsUpdater/2.0",
    "Accept": "application/json"
}


def get_existing_set_codes_from_db():
    """Recupera i set_code distinti attualmente presenti nella tabella 'cards'."""
    print("1. Connessione al database per verificare i set delle carte esistenti...")
    if not DATABASE_URL or "tuo_progetto" in DATABASE_URL:
        raise ValueError("ERRORE CRITICO: Configura una stringa DATABASE_URL o la variabile SUPABASE_DB_URL valida!")

    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 2, DATABASE_URL)
    conn = connection_pool.getconn()
    
    existing_sets = set()
    try:
        with conn.cursor() as cursor:
            # Query per estrarre tutti i set_code unici dalle carte salvate
            cursor.execute("SELECT DISTINCT set_code FROM public.cards WHERE set_code IS NOT NULL;")
            rows = cursor.fetchall()
            # Convertiamo tutti i codici in minuscolo per un confronto sicuro
            existing_sets = {row[0].lower() for row in rows}
            
        print(f"   Trovati {len(existing_sets)} set distinti nella tabella 'cards'.")
        return existing_sets
    except Exception as e:
        print(f"   ERRORE durante il recupero dei set dal DB: {e}")
        raise e
    finally:
        connection_pool.putconn(conn)
        connection_pool.closeall()


def get_scryfall_sets():
    """Scarica i metadati di tutti i set da Scryfall."""
    print("2. Download dei metadati dei set da Scryfall...")
    response = requests.get(SCRYFALL_SETS_URL, headers=HEADERS)
    response.raise_for_status()
    return response.json().get("data", [])


def download_svg_code(svg_url):
    """Scarica il codice sorgente (testo) del file SVG."""
    try:
        response = requests.get(svg_url, headers=HEADERS)
        response.raise_for_status()
        return response.text  # Ritorna il codice <svg> puro
    except Exception as e:
        print(f"   Errore download SVG ({svg_url}): {e}")
        return None


def process_sets(scryfall_sets, valid_set_codes):
    """Filtra i set di Scryfall e scarica gli SVG SOLO per quelli presenti nel DB."""
    print("3. Incrocio dati: filtraggio set ed estrazione SVG...")
    records = []
    skipped_count = 0
    
    for mtg_set in scryfall_sets:
        set_code = mtg_set.get("code", "").lower()
        name = mtg_set.get("name")
        set_type = mtg_set.get("set_type")
        released_at = mtg_set.get("released_at")
        svg_uri = mtg_set.get("icon_svg_uri")
        
        # 1. Ignora set privi di codice o di URL SVG
        if not set_code or not svg_uri:
            continue
            
        # 2. CONTROLLO FONDAMENTALE: Se il set NON esiste tra le carte nel DB, lo saltiamo!
        if set_code not in valid_set_codes:
            skipped_count += 1
            continue
            
        # Download del codice sorgente dell'SVG per i soli set validi
        print(f"   -> Scaricando SVG per il set: [{set_code.upper()}] {name}")
        svg_raw_code = download_svg_code(svg_uri)
        
        if not released_at:
            released_at = None
            
        records.append((
            set_code, 
            name, 
            set_type, 
            released_at, 
            svg_raw_code
        ))
        
        # Pausa di 150ms per rispettare i rate limits di Scryfall
        time.sleep(0.15)
        
    print(f"\n   Incrocio completato:")
    print(f"   - Set ignorati (non presenti in 'cards'): {skipped_count}")
    print(f"   - Set pronti per l'inserimento nel DB: {len(records)}")
    return records


def load_to_supabase(records):
    """Carica i dati filtrati su Supabase usando l'operazione di UPSERT."""
    if not records:
        print("4. Nessun set da aggiornare o inserire nel database.")
        return

    print("4. Connessione a Supabase per l'UPSERT dei set...")
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 2, DATABASE_URL)
    conn = connection_pool.getconn()
    
    try:
        with conn.cursor() as cursor:
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
            execute_values(cursor, upsert_query, records, page_size=100)
            
        conn.commit()
        print("5. 🎉 SUCCESS: Database dei set aggiornato con successo!")
        
    except Exception as e:
        print(f"   ERRORE DB: {e}")
        conn.rollback()
        raise e
    finally:
        connection_pool.putconn(conn)
        connection_pool.closeall()


def main():
    try:
        # Step 1: Recupera i set unici dalla tabella 'cards'
        valid_set_codes = get_existing_set_codes_from_db()
        
        if not valid_set_codes:
            print("Nessuna carta trovata nel database. Interruzione processo.")
            return

        # Step 2: Scarica la lista globale dei set da Scryfall
        scryfall_sets = get_scryfall_sets()

        # Step 3: Filtra e scarica solo gli SVG dei set rilevanti
        records = process_sets(scryfall_sets, valid_set_codes)

        # Step 4: Salva o aggiorna su Supabase
        load_to_supabase(records)

    except Exception as e:
        print(f"Errore critico durante l'esecuzione: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
