import ijson
import json
import os
import asyncio
import traceback
import libsql_client
from datetime import datetime

# File sorgente che verranno scaricati da GitHub Actions
INPUT_FILE = "AllIdentifiers.json"
SET_FILE = "SetList.json"

LAYOUT_SCARTATI = [
    "token", "double_faced_token", "emblem", "art_series", "vanguard", "planar", "scheme"
]
BATCH_SIZE = 500

async def aggiornamento_giornaliero():
    print("▶️ [START] Avvio routine di aggiornamento catalogo...")
    
    # 1. Recupero Credenziali da GitHub Secrets
    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")

    if not url or not token:
        print("❌ [ERRORE] Credenziali Turso non trovate nelle variabili d'ambiente!")
        return

    # 2. Lettura dei Set Validi
    oggi = datetime.now().strftime('%Y-%m-%d')
    set_futuri = {}
    
    if not os.path.exists(SET_FILE) or not os.path.exists(INPUT_FILE):
        print("❌ [ERRORE] File JSON mancanti. Assicurati che wget li abbia scaricati.")
        return

    print("📅 Lettura delle espansioni schedulate...")
    with open(SET_FILE, 'r', encoding='utf-8') as sf:
        set_data = json.load(sf)
        for espansione in set_data.get("data", []):
            data_uscita = espansione.get("releaseDate")
            codice_set = espansione.get("code")
            if data_uscita and data_uscita > oggi:
                set_futuri[codice_set] = True

    # 3. Estrazione delle Carte
    print("🔍 Filtraggio e preparazione delle carte valide in corso...")
    carte_da_inviare = []
    
    with open(INPUT_FILE, 'rb') as f:
        oggetti = ijson.kvitems(f, 'data')
        
        for card_uuid, card_data in oggetti:
            set_code = card_data.get("setCode")
            
            # Filtri (Spoiler, Terre, Funny, Layout)
            if set_code in set_futuri: continue
            if "Basic" in card_data.get("supertypes", []): continue
            if card_data.get("isFunny", False): continue
            if card_data.get("layout", "") in LAYOUT_SCARTATI: continue

            # Prepara la carta validata
            identifiers = card_data.get("identifiers", {})
            carte_da_inviare.append({
                "uuid": card_uuid,
                "name": card_data.get("name"),
                "set_code": set_code,
                "number": card_data.get("number"),
                "scryfall_id": identifiers.get("scryfallId"),
                "cardmarket_id": identifiers.get("cardmarketProductId"),
                "edhrec_rank": card_data.get("edhrecRank"),
                "track_price": 0
            })

    totale_carte = len(carte_da_inviare)
    print(f"✅ Trovate {totale_carte:,} carte legali totali. Connessione a Turso...")

    # 4. Invio al Database (OTTIMIZZATO)
    # INSERT OR IGNORE permette di non sprecare scritture per le carte già presenti
    sql = """
    INSERT OR IGNORE INTO cards (uuid, name, set_code, number, scryfall_id, cardmarket_id, edhrec_rank, track_price)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    try:
        async with libsql_client.create_client(url, auth_token=token) as client:
            for i in range(0, totale_carte, BATCH_SIZE):
                batch = carte_da_inviare[i : i + BATCH_SIZE]
                statements = []
                
                for c in batch:
                    args = [
                        c["uuid"], c["name"], c["set_code"], c["number"],
                        c["scryfall_id"], c["cardmarket_id"], c["edhrec_rank"], c["track_price"]
                    ]
                    statements.append(libsql_client.Statement(sql, args))
                
                await client.batch(statements)
                
        print("🎉 Aggiornamento giornaliero completato con successo!")
        print("Le nuove uscite (se presenti) sono state aggiunte, le carte vecchie sono state ignorate in modo efficiente.")
        
    except Exception as e:
        print(f"❌ [ERRORE DATABASE] Impossibile completare l'inserimento: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(aggiornamento_giornaliero())
