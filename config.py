import os
from dotenv import load_dotenv

load_dotenv()


def parse_int_list(value: str) -> set[int]:
    if not value:
        return set()

    result = set()
    for x in value.split(","):
        x = x.strip()
        if x.isdigit():
            result.add(int(x))
    return result


TOKEN = os.getenv("TOKEN", "").strip()
SERPAPI_KEY = os.getenv("SERPAPI_KEY", "").strip()
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "").strip()
PAPPERS_API_KEY = os.getenv("PAPPERS_API_KEY", "").strip()
INSEE_CLIENT_ID = os.getenv("INSEE_CLIENT_ID", "").strip()
INSEE_CLIENT_SECRET = os.getenv("INSEE_CLIENT_SECRET", "").strip()

OWNER_USER_ID = int(os.getenv("OWNER_USER_ID", "0") or "0")
ADMIN_USER_IDS = parse_int_list(os.getenv("ADMIN_USER_IDS", ""))

if OWNER_USER_ID:
    ADMIN_USER_IDS.add(OWNER_USER_ID)

if not TOKEN:
    raise RuntimeError("TOKEN manquant dans le fichier .env")