import psycopg2
from psycopg2.extras import execute_batch
import requests
import logging
import os
from datetime import datetime
from dotenv import load_dotenv

# =====================================================
# 📌 0. LOGGING SETUP
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# =====================================================
# 📌 1. LOAD CREDENTIALS FROM ENVIRONMENT
# =====================================================
load_dotenv()  # loads .env file when running locally

DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
EVENTS_URL  = os.getenv("EVENTS_URL")

if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, EVENTS_URL]):
    logger.error("Missing environment variables")
    exit(1)

# =====================================================
# 📌 2. SUPABASE CONNECTION
# =====================================================
try:
    conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    sslmode='require'
)
    cursor = conn.cursor()
    logger.info("Database connection established.")

except Exception as error:
    logger.error(f"Failed to connect to database: {error}")
    exit(1)

# =====================================================
# 📌 3. DATA EXTRACTION & INGESTION
# =====================================================
try:
    response    = requests.get(EVENTS_URL, timeout=30)
    dict_actual = response.json()

    eventos_vistos = set()
    poll_count     = 0

    while True:
        poll_count += 1
        lote_datos = []

        if "events" in dict_actual:
            for evento in dict_actual["events"]:

                # Only process tip events
                if evento.get("method") != "tip":
                    continue

                evento_id = evento["id"]

                # Deduplicate within session
                if evento_id in eventos_vistos:
                    logger.debug(f"Duplicate skipped: {evento_id}")
                    continue

                eventos_vistos.add(evento_id)

                usuario = evento["object"]["user"]["username"]
                tokens  = evento["object"]["tip"]["tokens"]
                hora    = datetime.now()

                lote_datos.append((evento_id, usuario, tokens, hora))
                logger.info(f"Tip received — {usuario}: {tokens} tokens")

        # Batch insert: one commit per poll (efficient)
        if lote_datos:
            execute_batch(
                cursor,
                "INSERT INTO tips_events (event_id, username, tokens, hour) VALUES (%s, %s, %s, %s)",
                lote_datos
            )
            conn.commit()
            logger.info(f"Batch of {len(lote_datos)} event(s) saved to Supabase.")

        # Follow pagination
        next_url    = dict_actual["nextUrl"]
        response    = requests.get(next_url, timeout=30)
        dict_actual = response.json()

except requests.exceptions.ConnectionError:
    logger.error("Connection lost. Restart the pipeline to resume.")

except Exception as e:
    logger.error(f"Unexpected error: {e}")

finally:
    if "cursor" in locals() and not cursor.closed:
        cursor.close()
    if "conn" in locals() and not conn.closed:
        conn.close()
    logger.info("Database connections closed safely.")
