import os
import time
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

# ==========================================
# CONFIGURAZIONI DATABASE & SCRAPER
# ==========================================
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

FORMATI = ["standard", "modern", "pioneer", "pauper", "legacy", "vintage"]
BASE_URL = "https://www.mtggoldfish.com"
NUMERO_MAZZI = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
}

def parse_card_line(line):
    """Divide la riga in quantità e nome della carta."""
    parts = line.strip().split(' ', 1)
    if len(parts) == 2 and parts[0].isdigit():
        return int(parts[0]), parts[1]
    return 1, line.strip()

def estrai_formato(formato, conn):
    metagame_url = f"{BASE_URL}/metagame/{formato}/full"
    
    print(f"\n{'='*40}")
    print(f"🔄 AVVIO SCANSIONE: {formato.upper()}")
    print(f"{'='*40}")
    
    response = requests.get(metagame_url, headers=HEADERS, timeout=10)
    if response.status_code != 200:
        print(f"❌ Errore metagame {formato}: Status {response.status_code}")
        return
        
    soup = BeautifulSoup(response.text, 'html.parser')
    mazzi_meta = soup.find_all('div', class_='archetype-tile')[:NUMERO_MAZZI]
    
    if not mazzi_meta:
        print(f"❌ Errore: Nessun mazzo trovato per {formato}. (Operazione annullata)")
        return

    # --- PULIZIA VECCHI DATI (DELETE NATIVO SQL) ---
    print(f"🧹 Pulizia dei vecchi mazzi {formato.upper()} nel database...")
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM public.decks WHERE format = %s;", (formato,))
        conn.commit()
        print(f"✅ Pulizia completata per {formato}.")
    except Exception as e:
        print(f"⚠️ Errore critico durante l'eliminazione dei vecchi record: {e}")
        conn.rollback()
        print("⏭️ Salto questo formato per evitare duplicati.")
        return

    # --- ESTRAZIONE E INSERIMENTO NUOVI DATI ---
    for indice, archetype in enumerate(mazzi_meta, start=1):
        
        # 1. Estrazione Dati Base Mazzo
        title_container = archetype.find('div', class_='archetype-tile-title')
        paper_span = title_container.find('span', class_='deck-price-paper') if title_container else None
        link_tag = paper_span.find('a') if paper_span else None
        
        if not link_tag or not link_tag.has_attr('href'):
            continue
            
        nome_mazzo = link_tag.text.strip()
        deck_url = urljoin(BASE_URL, link_tag['href'])
        
        # === ESTRAZIONE SINGOLA CARTA DI COPERTINA DALLA HOME ===
        carta_rappresentativa = None
        img_container = archetype.find('div', class_='archetype-tile-image')
        if img_container:
            card_tile = img_container.find('div', class_='card-tile')
            if card_tile:
                aria_label = card_tile.get('aria-label', '')
                if aria_label.startswith('Image of '):
                    carta_rappresentativa = aria_label.replace('Image of ', '').strip()
        
        # Estrazione percentuale meta
        stats_container = archetype.find('div', class_='archetype-tile-statistics')
        percentuale_val = None
        if stats_container:
            stats_values = stats_container.find_all('div', class_='archetype-tile-statistic-value')
            if stats_values:
                raw_perc = stats_values[0].text.strip().replace('%', '')
                try:
                    percentuale_val = float(raw_perc)
                except ValueError:
                    percentuale_val = None

        print(f"[{indice}/{len(mazzi_meta)}] Salvataggio: {nome_mazzo} (Copertina: {carta_rappresentativa})")
        
        # 2. Inserimento Mazzo su DB tramite SQL
        deck_id = None
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO public.decks (name, format, meta_percent, source_url, representative_card)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id;
                """, (nome_mazzo, formato, percentuale_val, deck_url, carta_rappresentativa))
                deck_id = cursor.fetchone()[0]
            conn.commit()
        except Exception as e:
            print(f"⚠️ Errore salvataggio {nome_mazzo} nel DB: {e}")
            conn.rollback()
            continue
        
        time.sleep(2) # Pausa per evitare rate-limiting
        
        # 3. Estrazione Decklist
        try:
            deck_page = requests.get(deck_url, headers=HEADERS, timeout=10)
            if deck_page.status_code == 200:
                deck_soup = BeautifulSoup(deck_page.text, 'html.parser')
                deck_input = deck_soup.find('input', id='deck_input_deck')
                
                if deck_input and 'value' in deck_input.attrs:
                    testo_mazzo = deck_input['value'].strip()
                    righe = testo_mazzo.split('\n')
                    
                    # Aggregazione delle quantità per nome carta per evitare duplicati
                    carte_aggregate = {}
                    nomi_univoci = set()
                    is_sideboard = False
                    
                    for riga in righe:
                        riga_pulita = riga.strip()
                        if not riga_pulita:
                            continue
                        
                        if riga_pulita.lower() == "sideboard":
                            is_sideboard = True
                            continue
                        
                        quantita, nome_carta = parse_card_line(riga_pulita)
                        
                        chiave = (nome_carta, is_sideboard)
                        if chiave in carte_aggregate:
                            carte_aggregate[chiave] += quantita
                        else:
                            carte_aggregate[chiave] = quantita
                            
                        nomi_univoci.add(nome_carta)
                    
                    # 4. BULK LOOKUP ID tramite query SQL nativa
                    mappa_id = {}
                    if nomi_univoci:
                        try:
                            with conn.cursor() as cursor:
                                cursor.execute("""
                                    SELECT name, scryfall_id 
                                    FROM public.cards 
                                    WHERE name = ANY(%s)
                                    ORDER BY price_eur ASC;
                                """, (list(nomi_univoci),))
                                
                                for row in cursor.fetchall():
                                    mappa_id[row[0]] = row[1]
                        except Exception as e:
                            print(f"⚠️ Errore durante la ricerca degli ID delle carte: {e}")
                            conn.rollback()
                    
                    # 5. Costruzione tuple per inserimento in blocco dal dizionario aggregato
                    carte_da_inserire = []
                    for (nome_carta, is_sb), quantita in carte_aggregate.items():
                        carte_da_inserire.append((
                            deck_id,
                            nome_carta,
                            mappa_id.get(nome_carta),
                            quantita,
                            is_sb
                        ))
                    
                    # 6. Inserimento massivo in deck_cards
                    if carte_da_inserire:
                        try:
                            with conn.cursor() as cursor:
                                insert_query = """
                                    INSERT INTO public.deck_cards (deck_id, card_name, scryfall_id, quantity, is_sideboard)
                                    VALUES %s;
                                """
                                execute_values(cursor, insert_query, carte_da_inserire)
                            conn.commit()
                        except Exception as e:
                            print(f"⚠️ Errore inserimento carte nel DB: {e}")
                            conn.rollback()
                            
            else:
                print(f"⚠️ Impossibile scaricare la lista di {nome_mazzo}.")
        except Exception as e:
            print(f"⚠️ Errore imprevisto estraendo {nome_mazzo}: {e}")

def main():
    if not DATABASE_URL:
        raise ValueError("CRITICAL ERROR: SUPABASE_DB_URL environment variable is missing!")

    print("🚀 INIZIO OPERAZIONE DI SCRAPING E SINCRONIZZAZIONE DB...")
    
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 5, DATABASE_URL)
    conn = connection_pool.getconn()
    
    try:
        for formato in FORMATI:
            estrai_formato(formato, conn)
            time.sleep(5)
            
        print("\n✅✅✅ TUTTE LE OPERAZIONI SONO CONCLUSE CON SUCCESSO!")
    finally:
        connection_pool.putconn(conn)
        connection_pool.closeall()

if __name__ == "__main__":
    main()
