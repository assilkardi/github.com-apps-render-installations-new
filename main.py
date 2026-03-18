import asyncio
import html
import json
import logging
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from config import TOKEN, SERPER_API_KEY, SERPAPI_KEY

# --------------------------------------------------
# BRANDING / UI
# --------------------------------------------------
BOT_BRAND_NAME = "LeadGen Premium"
BOT_BRAND_TAGLINE = "Prospection intelligente • Recherche ciblée • Export propre"
BOT_BRAND_ACCENT = "✨"
BOT_BRAND_SUPPORT = "Interface premium Telegram"

BRAND_IMAGE = os.getenv("BRAND_IMAGE", "").strip()

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
MAX_RESULTS = 200
DISPLAY_PAGE_SIZE = 20
COMPANY_PAGE_SIZE = 5
SERP_BATCH_SIZE = 10
MAX_MESSAGE_SAFE = 3800

EXPORT_DIR = "exports"
CACHE_FILE = "search_cache.json"
REQUEST_TIMEOUT = 20

MAX_COMPANY_STRONG_RESULTS = 5
MAX_COMPANY_TOTAL_RESULTS = 30

CACHE_TTL_DEFAULT = 1800
CACHE_TTL_COMPANY = 86400
CACHE_TTL_PERSON = 43200
CACHE_TTL_PROSPECT = 43200
CACHE_TTL_WEB = 86400
CACHE_TTL_ANNUAIRE = 86400

SEARCH_CACHE: Dict[str, Dict[str, object]] = {}
PROVIDER_STATS = {
    "cache": 0,
    "annuaire_api": 0,
    "serper": 0,
    "serpapi": 0,
}

ANNUAIRE_API_BASE = "https://recherche-entreprises.api.gouv.fr"
ANNUAIRE_USER_AGENT = "TelegramProspectBot/2.0"

# --------------------------------------------------
# LOGGING
# --------------------------------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.WARNING,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# --------------------------------------------------
# CACHE DISQUE
# --------------------------------------------------
def load_cache() -> None:
    global SEARCH_CACHE
    if not os.path.exists(CACHE_FILE):
        SEARCH_CACHE = {}
        return

    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            SEARCH_CACHE = json.load(f)
    except Exception as e:
        logger.warning("Impossible de charger le cache disque: %s", e)
        SEARCH_CACHE = {}


def save_cache() -> None:
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(SEARCH_CACHE, f, ensure_ascii=False)
    except Exception as e:
        logger.warning("Impossible de sauvegarder le cache disque: %s", e)


def get_cache_ttl(cache_key: str) -> int:
    if cache_key.startswith("company__"):
        return CACHE_TTL_COMPANY
    if cache_key.startswith("annuaire__"):
        return CACHE_TTL_ANNUAIRE
    if cache_key.startswith("person__"):
        return CACHE_TTL_PERSON
    if cache_key.startswith("prospect__"):
        return CACHE_TTL_PROSPECT
    if cache_key.startswith("web__"):
        return CACHE_TTL_WEB
    return CACHE_TTL_DEFAULT


def get_cache_payload(cache_key: str) -> Optional[Dict[str, object]]:
    cached = SEARCH_CACHE.get(cache_key)
    if not cached:
        return None

    now = time.time()
    ts = float(cached.get("ts", 0))
    ttl = get_cache_ttl(cache_key)

    if now - ts < ttl:
        PROVIDER_STATS["cache"] += 1
        return cached.get("data")

    return None


def set_cache_payload(cache_key: str, data: Dict[str, object]) -> None:
    SEARCH_CACHE[cache_key] = {
        "ts": time.time(),
        "data": data,
    }
    save_cache()

# --------------------------------------------------
# UI HELPERS
# --------------------------------------------------
def esc(text: Any) -> str:
    return html.escape(str(text or ""))


def brand_header_text() -> str:
    return (
        f"<b>{esc(BOT_BRAND_NAME)}</b>\n"
        f"{esc(BOT_BRAND_TAGLINE)}\n\n"
        f"{BOT_BRAND_ACCENT} <i>{esc(BOT_BRAND_SUPPORT)}</i>"
    )


def main_menu_text() -> str:
    return (
        f"<b>Bienvenue sur {esc(BOT_BRAND_NAME)}</b>\n\n"
        f"Choisis une action pour lancer une recherche :\n"
        f"• <b>Prospects LinkedIn</b>\n"
        f"• <b>Recherche personne</b>\n\n"
        f"Utilise ensuite les filtres pour affiner tes résultats."
    )


def prospects_intro_text() -> str:
    return (
        "<b>Module Prospects</b>\n\n"
        "Envoie un <b>mot-clé</b> à rechercher.\n"
        "Exemples :\n"
        "• commercial\n"
        "• marketing\n"
        "• recruteur\n"
        "• data analyst\n\n"
        "Tu peux aussi coller une <b>offre d’emploi</b> : le bot essaiera de trouver les profils les plus proches."
    )


def person_intro_text() -> str:
    return (
        "<b>Recherche personne</b>\n\n"
        "Choisis le type de recherche à effectuer :"
    )


def filters_help_text() -> str:
    return (
        "<b>Filtres optionnels</b>\n\n"
        "Format attendu :\n"
        "<code>ville=Paris,pays=France,entreprise=Google,poste=Sales,seniorite=manager</code>\n\n"
        "Tu peux aussi répondre simplement : <b>aucun</b>"
    )


def person_filters_help_text() -> str:
    return (
        "<b>Filtres optionnels</b>\n\n"
        "Format attendu :\n"
        "<code>ville=Paris,pays=France,entreprise=Airbus</code>\n\n"
        "Tu peux aussi répondre simplement : <b>aucun</b>"
    )


def excel_choice_text() -> str:
    return (
        "<b>Export Excel</b>\n\n"
        "Souhaites-tu générer également un <b>fichier Excel complet</b> ?"
    )


def loading_search_text(search_mode: str) -> str:
    if search_mode == "person":
        return "🔎 <b>Recherche LinkedIn en cours…</b>\nMerci de patienter quelques secondes."
    return "🔎 <b>Recherche prospects en cours…</b>\nPréparation des premiers résultats."


def company_search_loading_text() -> str:
    return "🏢 <b>Recherche entreprise en cours…</b>\nAnalyse des sources officielles et enrichissement web."


def no_result_text() -> str:
    return (
        "Aucun résultat exploitable trouvé.\n"
        "Essaie une autre orthographe, un autre mot-clé ou des filtres plus larges."
    )


def search_done_text(count: int, mode: str) -> str:
    if mode == "person":
        return f"✅ <b>{count}</b> profil(s) trouvé(s)."
    if mode == "company":
        return f"✅ <b>{count}</b> résultat(s) entreprise trouvé(s), classé(s) par pertinence."
    return f"✅ <b>{count}</b> profil(s) trouvé(s)."

# --------------------------------------------------
# CLAVIERS
# --------------------------------------------------
def menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Prospects LinkedIn", callback_data="menu_prospects")],
        [InlineKeyboardButton("👤 Recherche personne", callback_data="menu_person_menu")],
    ])


def person_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Recherche LinkedIn", callback_data="person_linkedin")],
        [InlineKeyboardButton("🏢 Recherche entreprise", callback_data="person_company")],
    ])


def excel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Oui, exporter", callback_data="excel_yes"),
            InlineKeyboardButton("➖ Non", callback_data="excel_no"),
        ]
    ])


def fuzzy_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Oui", callback_data="fuzzy_yes"),
            InlineKeyboardButton("❌ Non", callback_data="fuzzy_no"),
        ]
    ])


def pagination_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Afficher 20 résultats de plus", callback_data="more")]
    ])


def company_page_keyboard(current_page: int, total_results: int) -> InlineKeyboardMarkup:
    start_index = current_page * COMPANY_PAGE_SIZE
    end_index = min(start_index + COMPANY_PAGE_SIZE, total_results)

    rows = []
    detail_buttons = []

    for i in range(start_index, end_index):
        local_number = i - start_index + 1
        detail_buttons.append(
            InlineKeyboardButton(
                f"📄 Détails {local_number}",
                callback_data=f"company_detail_{i}",
            )
        )

    for i in range(0, len(detail_buttons), 2):
        rows.append(detail_buttons[i:i + 2])

    nav_row = []
    if current_page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Précédents", callback_data="company_prev"))
    if end_index < total_results:
        nav_row.append(InlineKeyboardButton("Suivants ➡️", callback_data="company_next"))

    if nav_row:
        rows.append(nav_row)

    return InlineKeyboardMarkup(rows)


def company_detail_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑️ Fermer", callback_data="company_back")]
    ])

# --------------------------------------------------
# OUTILS GÉNÉRAUX
# --------------------------------------------------
def sanitize_filename(text: str) -> str:
    text = re.sub(r"[^\w\-]+", "_", text.strip(), flags=re.UNICODE)
    return text[:80] or "recherche"


def normalize_linkedin_url(url: str) -> str:
    if not url:
        return ""
    return url.split("?")[0].strip().rstrip("/")


def split_long_message(text: str, max_len: int = MAX_MESSAGE_SAFE) -> List[str]:
    if len(text) <= max_len:
        return [text]

    chunks = []
    current = ""

    for block in text.split("\n\n"):
        if len(current) + len(block) + 2 <= max_len:
            current = f"{current}\n\n{block}".strip()
        else:
            if current:
                chunks.append(current)
            if len(block) <= max_len:
                current = block
            else:
                for i in range(0, len(block), max_len):
                    chunks.append(block[i:i + max_len])
                current = ""

    if current:
        chunks.append(current)

    return chunks


def parse_filters(text: str) -> Dict[str, str]:
    text = (text or "").strip()
    if not text or text.lower() == "aucun":
        return {}

    result = {}
    for part in text.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            k = k.strip().lower()
            v = v.strip()
            if k and v:
                result[k] = v
    return result


def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def normalize_text(text: str) -> str:
    text = strip_accents(text or "")
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s\-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def smart_extract_search_query(job_text: str) -> Dict[str, Any]:
    text = normalize_text(job_text)

    # 🔥 limiter taille
    words = text.split()
    text = " ".join(words[:50])

    job = ""
    keywords = []
    city = ""

    # 🎯 JOB DETECTION
    if any(x in text for x in ["fullstack", "full stack"]):
        job = "developpeur fullstack"
    elif any(x in text for x in ["backend"]):
        job = "developpeur backend"
    elif any(x in text for x in ["frontend"]):
        job = "developpeur frontend"
    elif any(x in text for x in ["data"]):
        job = "data analyst"
    elif any(x in text for x in ["devops"]):
        job = "devops engineer"
    elif any(x in text for x in ["sales", "commercial"]):
        job = "sales manager"

    # 🎯 TECH DETECTION
    tech_map = ["python", "java", "react", "node", "aws", "azure", "kubernetes"]
    for tech in tech_map:
        if tech in text:
            keywords.append(tech)

    # 🎯 VILLE
    cities = ["paris", "lyon", "lille", "marseille", "toulouse", "nantes"]
    for c in cities:
        if c in text:
            city = c
            break

    return {
        "job": job or "developpeur",
        "keywords": keywords[:3],
        "city": city
    }


def normalize_keyword(keyword: str) -> str:
    keyword = keyword.strip()
    replacements = {
        "java script": "JavaScript",
        "javascript": "JavaScript",
        "js developer": "JavaScript developer",
        "js engineer": "JavaScript engineer",
        "rh": "RH",
        "hr": "HR",
    }
    lowered = keyword.lower()
    for bad, good in replacements.items():
        if bad in lowered:
            keyword = re.sub(bad, good, keyword, flags=re.IGNORECASE)
    return keyword.strip()


async def safe_edit_message_text(
    message_obj,
    text: str,
    reply_markup=None,
    disable_web_page_preview: bool = True,
    parse_mode: Optional[str] = ParseMode.HTML,
) -> None:
    try:
        await message_obj.edit_text(
            text=text,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_web_page_preview,
            parse_mode=parse_mode,
        )
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        raise


def make_cache_key(
    search_mode: str,
    base_value: str,
    custom_filters: Dict[str, str],
    start: int,
    page_size: int,
    fuzzy: bool = False,
) -> str:
    filters_str = "|".join(f"{k}={v}" for k, v in sorted(custom_filters.items()))
    return f"{search_mode}__{base_value}__{filters_str}__{start}__{page_size}__{fuzzy}"


def make_web_cache_key(query: str, start: int, num: int) -> str:
    return f"web__{query}__{start}__{num}"


def parse_company_query_input(text: str) -> Tuple[str, Dict[str, str]]:
    text = (text or "").strip()
    if not text:
        return "", {}

    if "," not in text:
        return text, {}

    first_part, rest = text.split(",", 1)
    name = first_part.strip()
    filters_dict = parse_filters(rest)
    return name, filters_dict


def clean_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def first_nonempty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            cleaned = clean_spaces(value)
            if cleaned:
                return cleaned
        else:
            as_str = clean_spaces(str(value))
            if as_str:
                return as_str
    return ""


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default

# --------------------------------------------------
# NORMALISATION WEB SEARCH
# --------------------------------------------------
def normalize_organic_results(items: List[dict]) -> List[Dict[str, str]]:
    normalized = []

    for item in items or []:
        title = (item.get("title") or item.get("name") or "").strip()
        link = (
            item.get("link")
            or item.get("url")
            or item.get("href")
            or ""
        ).strip()
        snippet = (
            item.get("snippet")
            or item.get("description")
            or item.get("body")
            or ""
        ).strip()

        if not link:
            continue

        normalized.append({
            "title": title,
            "link": link,
            "snippet": snippet,
        })

    return normalized


def has_enough_results(result: Dict[str, object], minimum: int = 1) -> bool:
    organic = result.get("organic_results", []) or []
    return len(organic) >= minimum

# --------------------------------------------------
# PROVIDERS WEB SEARCH
# --------------------------------------------------
def search_with_serper(query: str, start: int = 0, num: int = 10) -> Dict[str, object]:
    if not SERPER_API_KEY:
        raise RuntimeError("SERPER_API_KEY manquante")

    page = (start // max(1, num)) + 1

    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json",
    }

    payload = {
        "q": query,
        "gl": "fr",
        "hl": "fr",
        "num": num,
        "page": page,
    }

    response = requests.post(
        "https://google.serper.dev/search",
        headers=headers,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()

    organic_results = normalize_organic_results(data.get("organic", []))
    PROVIDER_STATS["serper"] += 1

    return {
        "organic_results": organic_results,
        "has_more": len(organic_results) >= num,
        "next_start": start + num,
        "provider": "serper",
    }


def search_with_serpapi(query: str, start: int = 0, num: int = 10) -> Dict[str, object]:
    if not SERPAPI_KEY:
        raise RuntimeError("SERPAPI_KEY manquante")

    params = {
        "engine": "google",
        "q": query,
        "start": start,
        "num": num,
        "hl": "fr",
        "gl": "fr",
        "google_domain": "google.fr",
        "api_key": SERPAPI_KEY,
    }

    response = requests.get(
        "https://serpapi.com/search.json",
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()

    organic_results = normalize_organic_results(data.get("organic_results", []))
    serpapi_pagination = data.get("serpapi_pagination", {}) or {}
    next_link = serpapi_pagination.get("next")
    next_page_url = data.get("pagination", {}).get("next")
    has_more = bool(next_link or next_page_url)

    PROVIDER_STATS["serpapi"] += 1

    return {
        "organic_results": organic_results,
        "has_more": has_more,
        "next_start": start + num,
        "provider": "serpapi",
    }

def search_web(query: str, start: int = 0, num: int = 10) -> Dict[str, object]:
    cache_key = make_web_cache_key(query, start, num)
    cached = get_cache_payload(cache_key)
    if cached:
        return cached

    # 🔥 TRY SERPER
    try:
        result = search_with_serper(query, start=start, num=num)
        if result.get("organic_results"):
            set_cache_payload(cache_key, result)
            return result
    except Exception as e:
        logger.warning("Serper failed: %s", e)

    # 🔥 TRY SERPAPI
    try:
        result = search_with_serpapi(query, start=start, num=num)
        if result.get("organic_results"):
            set_cache_payload(cache_key, result)
            return result
    except Exception as e:
        logger.warning("SerpAPI failed: %s", e)

    # 🔥 FALLBACK
    return {
        "organic_results": [],
        "has_more": False,
        "next_start": start + num,
    }
# --------------------------------------------------
# PROSPECTS
# --------------------------------------------------
def get_keyword_aliases(keyword: str) -> List[str]:
    normalized = normalize_keyword(keyword)
    lower = normalized.lower()

    alias_map = {
        "rh": [
            "rh", "ressources humaines", "human resources", "hr",
            "talent acquisition", "recrutement", "charge de recrutement",
            "responsable recrutement"
        ],
        "hr": [
            "hr", "human resources", "ressources humaines",
            "talent acquisition", "recrutement"
        ],
        "rrh": ["rrh", "responsable ressources humaines", "human resources manager"],
        "drh": ["drh", "directeur ressources humaines", "directrice ressources humaines", "hr director"],
        "commercial": [
            "commercial", "sales", "business development", "bizdev",
            "account executive", "charge d affaires", "ingénieur d affaires",
            "ingenieur d affaires"
        ],
        "marketing": [
            "marketing", "growth", "brand", "digital marketing",
            "content marketing", "communication", "marketing digital"
        ],
        "finance": ["finance", "financial", "comptabilite", "accounting", "controle de gestion"],
        "data": ["data", "data analyst", "data scientist", "business intelligence", "bi"],
        "recruteur": ["recruteur", "recruiter", "talent acquisition", "charge de recrutement"],
    }

    if lower in alias_map:
        return alias_map[lower]

    return [normalized]


def contains_alias(text: str, aliases: List[str]) -> bool:
    haystack = normalize_text(text)
    if not haystack:
        return False

    for alias in aliases:
        alias = normalize_text(alias)
        if not alias:
            continue

        if len(alias) <= 3 and " " not in alias:
            pattern = rf"\b{re.escape(alias)}\b"
            if re.search(pattern, haystack, flags=re.IGNORECASE):
                return True
        else:
            if alias in haystack:
                return True

    return False


def row_matches_keyword(row: Dict[str, str], keyword: str) -> bool:
    aliases = get_keyword_aliases(keyword)

    poste = row.get("Poste", "") or ""
    entreprise = row.get("Entreprise", "") or ""
    snippet = row.get("Snippet", "") or ""

    strong_text = " ".join([poste, entreprise, snippet]).strip()
    return contains_alias(strong_text, aliases)


def build_prospect_query(keyword: str, custom_filters: Dict[str, str]) -> str:
    keyword = normalize_keyword(keyword)
    normalized_keyword = normalize_text(keyword)

    aliases = get_keyword_aliases(keyword)

    # 🔥 si plusieurs mots, on construit une requête plus souple
    keyword_tokens = [t for t in normalized_keyword.split() if len(t) >= 2]

    if len(aliases) > 1:
        keyword_part = "(" + " OR ".join(f'"{a}"' for a in aliases) + ")"
    elif len(keyword_tokens) >= 2:
        keyword_part = " ".join(f'"{t}"' for t in keyword_tokens[:4])
    else:
        keyword_part = f'"{aliases[0]}"'

    query = (
        f'site:linkedin.com/in {keyword_part} '
        f'-jobs -job -hiring -recruitment -recrutement '
        f'-offres -offre -emploi -stage -alternance -apprentissage '
        f'-learning -formation -formations -posts -school -ecole -universite'
    )

    entreprise = custom_filters.get("entreprise")
    ville = custom_filters.get("ville")
    pays = custom_filters.get("pays")
    secteur = custom_filters.get("secteur")
    poste = custom_filters.get("poste")
    seniorite = custom_filters.get("seniorite")

    if poste:
        query += f' "{poste}"'
    if entreprise:
        query += f' "{entreprise}"'
    if ville:
        query += f' "{ville}"'
    if pays:
        query += f' "{pays}"'
    if secteur:
        query += f' "{secteur}"'
    if seniorite:
        query += f' "{seniorite}"'

    return query

def is_job_offer(text: str) -> bool:
    text = normalize_text(text)

    keywords = [
        "recherche", "poste", "mission", "profil", "experience",
        "responsable", "manager", "recrutons", "offre", "job",
        "role", "position", "cdi", "cdd", "alternance", "stage",
        "candidat", "entreprise", "client", "competences", "qualification"
    ]

    return any(k in text for k in keywords)


def extract_job_params(job_text: str) -> Dict[str, Any]:
    text = normalize_text(job_text)

    job_titles: List[str] = []
    keywords: List[str] = []
    experience_years = 0
    words = text.split()
    text = " ".join(words[:30])

    if any(x in text for x in ["sales", "commercial", "business developer", "bizdev"]):
        job_titles += ["sales manager", "account executive", "business developer", "commercial"]

    if any(x in text for x in ["marketing", "growth"]):
        job_titles += ["marketing manager", "growth manager"]

    if any(x in text for x in ["data", "bi"]):
        job_titles += ["data analyst", "data scientist"]

    if "saas" in text:
        keywords.append("saas")

    if "b2b" in text:
        keywords.append("b2b")

    # 🔥 EXPERIENCE
    exp_match = re.search(r"(\d+)\s*(ans|years)", text)
    if exp_match:
        experience_years = int(exp_match.group(1))

    city_match = re.search(r"(paris|lyon|marseille|lille)", text)
    city = city_match.group(1) if city_match else ""

    if not job_titles:
        job_titles = [job_text]

    return {
        "job_titles": job_titles,
        "keywords": keywords,
        "city": city,
        "experience": experience_years,  # 🔥 NEW
    }


def build_multi_queries(params: Dict[str, Any]) -> List[str]:
    queries = []

    for title in params.get("job_titles", []):
        parts = [title]
        parts.extend(params.get("keywords", []))
        if params.get("city"):
            parts.append(params["city"])
        base = clean_spaces(" ".join(parts))
        if base:
            queries.append(base)

    if not queries and params.get("job_titles"):
        queries = params["job_titles"][:]

    return list(dict.fromkeys(queries))

def extract_experience_from_text(text: str) -> int:
    match = re.search(r"(\d+)\s*(ans|years)", text.lower())
    return int(match.group(1)) if match else 0

def row_matches_job_offer(row: Dict[str, str], params: Dict[str, Any]) -> bool:
    text = normalize_text(" ".join([
        row.get("Poste", ""),
        row.get("Entreprise", ""),
        row.get("Snippet", ""),
    ]))

    title_hit = False
    for t in params.get("job_titles", []):
        nt = normalize_text(t)
        if nt and nt in text:
            title_hit = True
            break

    keyword_hits = 0
    for k in params.get("keywords", []):
        nk = normalize_text(k)
        if nk and nk in text:
            keyword_hits += 1

    if title_hit:
        return True
    if keyword_hits >= 1:
        return True

    return False


def score_profile_advanced(row: Dict[str, str], params: Dict[str, Any], custom_filters: Dict[str, str]) -> int:
    score = 0

    title = normalize_text(row.get("Poste", ""))
    snippet = normalize_text(row.get("Snippet", ""))
    combined = f"{title} {snippet}"

    # 🎯 TITRE
    for t in params.get("job_titles", []):
        nt = normalize_text(t)
        if nt in title:
            score += 50
            break
        elif nt in combined:
            score += 25

    # 🎯 KEYWORDS
    for k in params.get("keywords", []):
        nk = normalize_text(k)
        if nk in combined:
            score += 15

    # 🎯 VILLE
    if params.get("city") and params["city"] in combined:
        score += 20

    # 🔥 EXPERIENCE
    required_exp = params.get("experience", 0)
    profile_exp = extract_experience_from_text(snippet)

    if required_exp:
        if profile_exp >= required_exp:
            score += 25
        else:
            score -= 10

    return score

# --------------------------------------------------
# RECHERCHE PERSONNE LINKEDIN
# --------------------------------------------------
def split_name(full_name: str) -> List[str]:
    return [p for p in normalize_text(full_name).split() if p]


def generate_first_name_variants(first_name: str) -> List[str]:
    first = normalize_text(first_name)
    variants = {first}

    custom_map = {
        "sarah": {"sarah", "sara", "sarra"},
        "sarra": {"sarra", "sara", "sarah"},
        "sara": {"sara", "sarah", "sarra"},
        "mohamed": {"mohamed", "mohammed", "muhammad", "mohamad"},
        "mohammed": {"mohammed", "mohamed", "muhammad", "mohamad"},
        "amina": {"amina", "aminah"},
        "aminah": {"aminah", "amina"},
    }

    if first in custom_map:
        variants.update(custom_map[first])

    if first.endswith("ah"):
        variants.add(first[:-1])
    if first.endswith("a"):
        variants.add(first + "h")
    if "rr" in first:
        variants.add(first.replace("rr", "r"))
    if "r" in first and "rr" not in first:
        variants.add(first.replace("r", "rr", 1))

    return [v for v in variants if v]


def build_person_query(person_name: str, custom_filters: Dict[str, str], fuzzy_enabled: bool) -> str:
    parts = split_name(person_name)

    if not parts:
        return ""

    first = parts[0]
    last = parts[-1]

    name_variants = []

    if fuzzy_enabled:
        variants = generate_first_name_variants(first)
        for v in variants:
            name_variants.append(f'"{v} {last}"')
    else:
        name_variants.append(f'"{first} {last}"')

    name_query = "(" + " OR ".join(name_variants) + ")"
    query = (
        f"site:linkedin.com/in {name_query} "
        f"-jobs -job -hiring -recruitment -recrutement "
        f"-offres -offre -emploi -stage -alternance -apprentissage "
        f"-posts -school -ecole -universite"
    )

    entreprise = custom_filters.get("entreprise")
    ville = custom_filters.get("ville")
    pays = custom_filters.get("pays")

    if entreprise:
        query += f' "{entreprise}"'
    if ville:
        query += f' "{ville}"'
    if pays:
        query += f' "{pays}"'

    return query


def compute_person_match_score(result_name: str, target_name: str, fuzzy_enabled: bool) -> float:
    result_parts = split_name(result_name)
    target_parts = split_name(target_name)

    if not result_parts or not target_parts:
        return 0.0

    result_full = " ".join(result_parts)
    target_full = " ".join(target_parts)
    full_ratio = SequenceMatcher(None, result_full, target_full).ratio()

    if not fuzzy_enabled:
        return full_ratio if result_full == target_full else 0.0

    result_first = result_parts[0]
    target_first = target_parts[0]
    result_last = result_parts[-1]
    target_last = target_parts[-1]

    first_ratio = SequenceMatcher(None, result_first, target_first).ratio()
    last_ratio = SequenceMatcher(None, result_last, target_last).ratio()

    return (first_ratio * 0.45) + (last_ratio * 0.45) + (full_ratio * 0.10)


def row_matches_person(row: Dict[str, str], target_name: str, fuzzy_enabled: bool) -> bool:
    score = compute_person_match_score(row.get("Nom", ""), target_name, fuzzy_enabled)
    row["MatchScore"] = round(score, 2)

    if fuzzy_enabled:
        return score >= 0.65
    return score >= 0.80

# --------------------------------------------------
# ENTREPRISES - OFFICIEL + ENRICHISSEMENT
# --------------------------------------------------
def build_annuaire_search_params(name: str, ville_filter: str = "", page: int = 1, per_page: int = 10) -> Dict[str, str]:
    q = clean_spaces(f"{name} {ville_filter}".strip())
    return {
        "q": q,
        "page": str(page),
        "per_page": str(per_page),
        "etat_administratif": "A",
    }


def build_annuaire_source_link(siren: str) -> str:
    siren = re.sub(r"\D", "", siren or "")
    if len(siren) == 9:
        return f"https://annuaire-entreprises.data.gouv.fr/entreprise/{siren}"
    return ""


def format_dirigeant_name(dirigeant: dict) -> str:
    if not isinstance(dirigeant, dict):
        return ""
    return clean_spaces(" ".join([
        str(dirigeant.get("prenoms", "") or ""),
        str(dirigeant.get("nom", "") or ""),
    ])).strip()


def extract_annuaire_dirigeants(item: dict) -> str:
    dirigeants = item.get("dirigeants") or []
    if not isinstance(dirigeants, list):
        return ""

    names = []
    for d in dirigeants[:3]:
        name = format_dirigeant_name(d)
        if name:
            qualite = clean_spaces(str(d.get("qualite", "") or ""))
            if qualite:
                names.append(f"{name} ({qualite})")
            else:
                names.append(name)

    return " ; ".join(names)


def extract_annuaire_activity(item: dict) -> str:
    return first_nonempty(
        item.get("libelle_activite_principale"),
        item.get("activite_principale"),
    )


def extract_annuaire_city(item: dict) -> str:
    siege = item.get("siege") or {}
    return first_nonempty(
        siege.get("libelle_commune"),
        item.get("libelle_commune"),
    )


def extract_annuaire_address(item: dict) -> str:
    siege = item.get("siege") or {}
    return first_nonempty(
        siege.get("adresse_complete"),
        clean_spaces(" ".join([
            str(siege.get("numero_voie", "") or ""),
            str(siege.get("type_voie", "") or ""),
            str(siege.get("libelle_voie", "") or ""),
            str(siege.get("code_postal", "") or ""),
            str(siege.get("libelle_commune", "") or ""),
        ])),
    )


def extract_annuaire_company_name(item: dict) -> str:
    return first_nonempty(
        item.get("nom_complet"),
        item.get("nom_raison_sociale"),
        item.get("denomination"),
        item.get("sigle"),
    )


def extract_annuaire_creation_date(item: dict) -> str:
    return first_nonempty(item.get("date_creation"))


def compute_company_info_score(row: Dict[str, str]) -> int:
    fields = [
        "Entreprise_INPI",
        "SIREN",
        "Dirigeant",
        "Ville_INPI",
        "Adresse_INPI",
        "Lien_source",
        "Snippet_entreprise",
        "Activite",
        "Date_creation",
    ]
    score = 0
    for field in fields:
        if (row.get(field, "") or "").strip():
            score += 1
    return score


def compute_company_relevance(row: Dict[str, str], target_name: str, ville_filter: str = "") -> Tuple[int, int]:
    parts = split_name(target_name)
    full_name = " ".join(parts).strip()
    last_name = parts[-1] if parts else target_name.strip()

    searchable_text = " ".join([
        row.get("Entreprise_INPI", "") or "",
        row.get("Dirigeant", "") or "",
        row.get("Ville_INPI", "") or "",
        row.get("Adresse_INPI", "") or "",
        row.get("Snippet_entreprise", "") or "",
        row.get("Lien_source", "") or "",
    ])

    haystack = normalize_text(searchable_text)
    ville_norm = normalize_text(ville_filter or "")
    full_name_norm = normalize_text(full_name)
    last_name_norm = normalize_text(last_name)

    full_name_match = bool(full_name_norm and full_name_norm in haystack)
    last_name_match = bool(last_name_norm and re.search(rf"\b{re.escape(last_name_norm)}\b", haystack))
    ville_match = bool(ville_norm and ville_norm in haystack)

    if full_name_match and ville_match:
        relevance = 3
        label = "Nom + prénom + ville"
    elif full_name_match:
        relevance = 2
        label = "Nom + prénom"
    elif last_name_match:
        relevance = 1
        label = "Nom"
    else:
        relevance = 0
        label = "Approx"

    info_score = compute_company_info_score(row)
    row["RelevanceLabel"] = label
    row["InfoScore"] = info_score

    return relevance, info_score


def annuaire_item_to_row(item: dict, query_name: str, ville_filter: str = "") -> Dict[str, str]:
    company_name = extract_annuaire_company_name(item)
    siren = first_nonempty(item.get("siren"))
    city = extract_annuaire_city(item)
    address = extract_annuaire_address(item)
    dirigeant = extract_annuaire_dirigeants(item)
    activite = extract_annuaire_activity(item)
    date_creation = extract_annuaire_creation_date(item)

    row = {
        "Entreprise_INPI": company_name or "N/A",
        "SIREN": siren or "",
        "Ville_INPI": city or "",
        "Adresse_INPI": address or "",
        "Dirigeant": dirigeant or "",
        "Activite": activite or "",
        "Date_creation": date_creation or "",
        "Source_entreprise": "annuaire_api",
        "Lien_source": build_annuaire_source_link(siren),
        "Snippet_entreprise": first_nonempty(
            activite,
            address,
            city,
            company_name,
        ),
    }

    relevance, info_score = compute_company_relevance(row, query_name, ville_filter)
    row["RelevanceScore"] = relevance
    row["InfoScore"] = info_score
    return row


def search_company_annuaire(name: str, ville_filter: str = "", max_results: int = 12) -> List[Dict[str, str]]:
    cache_key = f"annuaire__{normalize_text(name)}__{normalize_text(ville_filter)}__{max_results}"
    cached = get_cache_payload(cache_key)
    if cached:
        return cached.get("results", [])

    headers = {
        "Accept": "application/json",
        "User-Agent": ANNUAIRE_USER_AGENT,
    }

    params = build_annuaire_search_params(name, ville_filter, page=1, per_page=max_results)

    response = requests.get(
        f"{ANNUAIRE_API_BASE}/search",
        headers=headers,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()

    rows = []
    for item in data.get("results", []) or []:
        if isinstance(item, dict):
            rows.append(annuaire_item_to_row(item, name, ville_filter))

    rows.sort(
        key=lambda r: (
            safe_int(r.get("RelevanceScore", 0)),
            safe_int(r.get("InfoScore", 0)),
            len((r.get("Dirigeant", "") or "").strip()),
            len((r.get("Adresse_INPI", "") or "").strip()),
        ),
        reverse=True,
    )

    PROVIDER_STATS["annuaire_api"] += 1
    payload = {"results": rows[:max_results]}
    set_cache_payload(cache_key, payload)
    return payload["results"]


def build_company_queries(name: str, ville_filter: str = "") -> List[str]:
    parts = split_name(name)
    exact_name = " ".join(parts).strip()
    last_name = parts[-1] if parts else name.strip()

    base_queries: List[str] = []

    if exact_name:
        if ville_filter:
            base_queries.extend([
                f'site:annuaire-entreprises.data.gouv.fr/entreprise "{exact_name}" "{ville_filter}"',
                f'site:societe.com "{exact_name}" "{ville_filter}"',
                f'site:pappers.fr "{exact_name}" "{ville_filter}"',
                f'site:verif.com "{exact_name}" "{ville_filter}"',
                f'site:annuaire-entreprises.data.gouv.fr "{exact_name}" dirigeant "{ville_filter}"',
            ])
        else:
            base_queries.extend([
                f'site:annuaire-entreprises.data.gouv.fr/entreprise "{exact_name}"',
                f'site:societe.com "{exact_name}"',
                f'site:pappers.fr "{exact_name}"',
                f'site:verif.com "{exact_name}"',
                f'site:annuaire-entreprises.data.gouv.fr "{exact_name}" dirigeant',
                f'site:societe.com "{exact_name}" dirigeant',
            ])

    if last_name and last_name.lower() != exact_name.lower():
        if ville_filter:
            base_queries.extend([
                f'site:annuaire-entreprises.data.gouv.fr/entreprise "{last_name}" "{ville_filter}"',
                f'site:societe.com "{last_name}" "{ville_filter}"',
            ])
        else:
            base_queries.extend([
                f'site:annuaire-entreprises.data.gouv.fr/entreprise "{last_name}"',
                f'site:societe.com "{last_name}"',
            ])

    return list(dict.fromkeys(base_queries))[:6]


def is_company_domain(link: str) -> bool:
    if not link:
        return False
    link = link.lower()
    return any(domain in link for domain in [
        "annuaire-entreprises.data.gouv.fr",
        "societe.com",
        "pappers.fr",
        "manageo.fr",
        "verif.com",
    ])


def extract_siren(text: str) -> str:
    if not text:
        return ""
    cleaned = text.replace(" ", "").replace(".", "")
    match = re.search(r"\b(\d{9})\b", cleaned)
    return match.group(1) if match else ""


def extract_city_from_text(text: str) -> str:
    if not text:
        return ""
    match = re.search(r"\b\d{5}\s+([A-Za-zÀ-ÿ\-\s']{2,40})", text)
    if match:
        return match.group(1).strip(" ,.-")
    return ""


def extract_address_from_text(text: str) -> str:
    if not text:
        return ""
    match = re.search(r"([0-9]{1,4}[^.:\n]{5,160}\b\d{5}\s+[A-Za-zÀ-ÿ\-\s']{2,40})", text)
    if match:
        return match.group(1).strip(" ,.-")
    return ""


def extract_company_name_from_title(title: str) -> str:
    if not title:
        return ""

    title = title.strip()
    patterns_to_remove = [
        " - Annuaire des entreprises",
        "| Annuaire des entreprises",
        "Annuaire des entreprises",
        " - Société.com",
        "| Société.com",
        " - Pappers",
        "| Pappers",
        " - Manageo.fr",
        "| Manageo.fr",
        " - Manageo",
        "| Manageo",
        " - Verif.com",
        "| Verif.com",
        " - Verif",
        "| Verif",
    ]

    for p in patterns_to_remove:
        title = title.replace(p, "")

    return re.sub(r"\s+", " ", title).strip(" -|")


def extract_dirigeant_from_text(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"\s+", " ", text)

    patterns = [
        r"(?:dirigeant|gérant|gerant|président|president|représentant légal|representant legal|présidente|presidente|président du conseil|chef d entreprise)\s*[:\-]?\s*([A-ZÀ-Ÿ][A-Za-zÀ-ÿ'\-]+\s+[A-ZÀ-Ÿ][A-Za-zÀ-ÿ'\-]+)",
        r"([A-ZÀ-Ÿ][A-Za-zÀ-ÿ'\-]+\s+[A-ZÀ-Ÿ][A-Za-zÀ-ÿ'\-]+)\s*(?:dirigeant|gérant|gerant|président|president|présidente|presidente)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()

    return ""


def count_strong_company_results(rows: List[Dict[str, str]]) -> int:
    count = 0
    for row in rows:
        if safe_int(row.get("RelevanceScore", 0)) >= 2 and safe_int(row.get("InfoScore", 0)) >= 5:
            count += 1
    return count


def search_company_web_query(query: str, name: str, ville_filter: str = "") -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    ville_filter_norm = normalize_text(ville_filter or "")

    data = search_web(query, start=0, num=10)
    organic_results = data.get("organic_results", [])

    for item in organic_results:
        link = (item.get("link", "") or "").strip()
        title = (item.get("title", "") or "").strip()
        snippet = (item.get("snippet", "") or "").strip()

        if not link:
            continue
        if not is_company_domain(link):
            continue

        combined_text = " ".join([title, snippet, link])

        row = {
            "Entreprise_INPI": extract_company_name_from_title(title) or title[:120] or "N/A",
            "SIREN": extract_siren(combined_text),
            "Ville_INPI": extract_city_from_text(combined_text),
            "Adresse_INPI": extract_address_from_text(combined_text),
            "Dirigeant": extract_dirigeant_from_text(combined_text),
            "Activite": "",
            "Date_creation": "",
            "Source_entreprise": data.get("provider", "web"),
            "Lien_source": link,
            "Snippet_entreprise": snippet,
        }

        if ville_filter_norm:
            haystack = normalize_text(
                f"{row.get('Ville_INPI', '')} {row.get('Adresse_INPI', '')} {title} {snippet}"
            )
            if ville_filter_norm not in haystack:
                continue

        relevance, info_score = compute_company_relevance(row, name, ville_filter)
        row["RelevanceScore"] = relevance
        row["InfoScore"] = info_score
        results.append(row)

    return results


def search_company_web_enrichment(name: str, ville_filter: str = "") -> List[Dict[str, str]]:
    queries = build_company_queries(name, ville_filter)
    collected: List[Dict[str, str]] = []
    seen_keys = set()

    max_workers = min(4, len(queries)) or 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(search_company_web_query, q, name, ville_filter): q
            for q in queries
        }

        for future in as_completed(futures):
            try:
                rows = future.result()
            except Exception as e:
                logger.warning("Erreur enrichissement requête web: %s", e)
                continue

            for row in rows:
                siren = (row.get("SIREN", "") or "").strip()
                name_key = normalize_text(row.get("Entreprise_INPI", "") or "")
                city_key = normalize_text(row.get("Ville_INPI", "") or "")
                key = siren or f"{name_key}__{city_key}"

                if key in seen_keys:
                    continue

                seen_keys.add(key)
                collected.append(row)

                if len(collected) >= MAX_COMPANY_TOTAL_RESULTS:
                    break

            if count_strong_company_results(collected) >= MAX_COMPANY_STRONG_RESULTS:
                break
            if len(collected) >= MAX_COMPANY_TOTAL_RESULTS:
                break

    collected.sort(
        key=lambda r: (
            safe_int(r.get("RelevanceScore", 0)),
            safe_int(r.get("InfoScore", 0)),
            len((r.get("Dirigeant", "") or "").strip()),
            len((r.get("Adresse_INPI", "") or "").strip()),
        ),
        reverse=True,
    )

    return collected[:MAX_COMPANY_TOTAL_RESULTS]


def merge_company_sources(primary_rows: List[Dict[str, str]], secondary_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    merged: List[Dict[str, str]] = []
    seen_keys = set()

    for row in primary_rows + secondary_rows:
        siren = (row.get("SIREN", "") or "").strip()
        name_key = normalize_text(row.get("Entreprise_INPI", "") or "")
        city_key = normalize_text(row.get("Ville_INPI", "") or "")
        key = siren or f"{name_key}__{city_key}"

        if key in seen_keys:
            continue

        seen_keys.add(key)
        merged.append(row)

    merged.sort(
        key=lambda r: (
            safe_int(r.get("RelevanceScore", 0)),
            safe_int(r.get("InfoScore", 0)),
            1 if r.get("Source_entreprise") == "annuaire_api" else 0,
            len((r.get("Dirigeant", "") or "").strip()),
            len((r.get("Adresse_INPI", "") or "").strip()),
        ),
        reverse=True,
    )

    return merged[:MAX_COMPANY_TOTAL_RESULTS]


def search_company_person(name: str, ville_filter: str = "") -> List[Dict[str, str]]:
    cache_key = f"company__{normalize_text(name)}__{normalize_text(ville_filter)}"
    cached = get_cache_payload(cache_key)
    if cached:
        return cached.get("results", [])

    annuaire_rows: List[Dict[str, str]] = []
    web_rows: List[Dict[str, str]] = []

    try:
        annuaire_rows = search_company_annuaire(name, ville_filter, max_results=12)
    except Exception as e:
        logger.warning("Annuaire API indisponible: %s", e)

    if count_strong_company_results(annuaire_rows) < MAX_COMPANY_STRONG_RESULTS:
        try:
            web_rows = search_company_web_enrichment(name, ville_filter)
        except Exception as e:
            logger.warning("Enrichissement web indisponible: %s", e)

    final_rows = merge_company_sources(annuaire_rows, web_rows)
    payload = {"results": final_rows[:MAX_COMPANY_TOTAL_RESULTS]}
    set_cache_payload(cache_key, payload)
    return payload["results"]

# --------------------------------------------------
# AFFICHAGE ENTREPRISE
# --------------------------------------------------
def build_company_summary_line(global_index: int, row: Dict[str, str]) -> str:
    entreprise = esc(row.get("Entreprise_INPI", "") or "N/A")
    siren = esc(row.get("SIREN", "") or "N/A")
    pertinence = esc(row.get("RelevanceLabel", "") or "N/A")
    dirigeant = esc(row.get("Dirigeant", "") or "N/A")
    ville = esc(row.get("Ville_INPI", "") or "N/A")
    source = esc(row.get("Source_entreprise", "") or "N/A")

    return (
        f"<b>{global_index}. {entreprise}</b>\n"
        f"• <b>SIREN :</b> {siren}\n"
        f"• <b>Pertinence :</b> {pertinence}\n"
        f"• <b>Dirigeant :</b> {dirigeant}\n"
        f"• <b>Ville :</b> {ville}\n"
        f"• <b>Source :</b> {source}"
    )


def build_company_detail_text(index: int, row: Dict[str, str]) -> str:
    return (
        f"<b>Détails du résultat {index}</b>\n\n"
        f"• <b>Entreprise :</b> {esc(row.get('Entreprise_INPI', '') or 'N/A')}\n"
        f"• <b>SIREN :</b> {esc(row.get('SIREN', '') or 'N/A')}\n"
        f"• <b>Pertinence :</b> {esc(row.get('RelevanceLabel', '') or 'N/A')}\n"
        f"• <b>Dirigeant :</b> {esc(row.get('Dirigeant', '') or 'N/A')}\n"
        f"• <b>Adresse :</b> {esc(row.get('Adresse_INPI', '') or 'N/A')}\n"
        f"• <b>Ville :</b> {esc(row.get('Ville_INPI', '') or 'N/A')}\n"
        f"• <b>Activité :</b> {esc(row.get('Activite', '') or 'N/A')}\n"
        f"• <b>Date création :</b> {esc(row.get('Date_creation', '') or 'N/A')}\n"
        f"• <b>Source :</b> {esc(row.get('Source_entreprise', '') or 'N/A')}\n"
        f"• <b>Lien :</b> {esc(row.get('Lien_source', '') or 'N/A')}\n\n"
        f"<b>Snippet</b>\n{esc(row.get('Snippet_entreprise', '') or 'N/A')}"
    )


def build_company_page_text(companies: List[Dict[str, str]], current_page: int) -> str:
    start_index = current_page * COMPANY_PAGE_SIZE
    subset = companies[start_index:start_index + COMPANY_PAGE_SIZE]

    if not subset:
        return "Plus de résultats disponibles."

    lines = []
    for i, row in enumerate(subset, start=start_index + 1):
        lines.append(build_company_summary_line(i, row))

    total_pages = (len(companies) + COMPANY_PAGE_SIZE - 1) // COMPANY_PAGE_SIZE
    header = f"<b>Résultats entreprise</b> — page <b>{current_page + 1}/{total_pages}</b>\n\n"
    return header + "\n\n".join(lines)

# --------------------------------------------------
# PARSING TITRE GOOGLE
# --------------------------------------------------
def parse_google_title(title: str) -> Dict[str, str]:
    cleaned = title.replace("| LinkedIn", "").strip()
    parts = [p.strip() for p in cleaned.split(" - ")]

    nom = parts[0] if len(parts) > 0 else ""
    poste = parts[1] if len(parts) > 1 else ""
    entreprise = parts[2] if len(parts) > 2 else ""

    return {
        "Nom": nom,
        "Poste": poste,
        "Entreprise": entreprise,
    }

# --------------------------------------------------
# EXCEL
# --------------------------------------------------
def export_excel(results: List[Dict[str, str]], keyword: str) -> str:
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.worksheet.table import Table, TableStyleInfo
    from openpyxl.utils import get_column_letter

    Path(EXPORT_DIR).mkdir(exist_ok=True)
    safe_keyword = sanitize_filename(keyword)
    file_name = f"{EXPORT_DIR}/prospects_{safe_keyword}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    final_columns = [
        "Nom",
        "Poste",
        "Entreprise",
        "Entreprise_INPI",
        "SIREN",
        "Dirigeant",
        "Ville_INPI",
        "Adresse_INPI",
        "Ville",
        "Pays",
        "Source",
        "Statut",
        "Priorité",
        "MatchScore",
        "Notes",
        "Snippet",
        "LinkedIn",
        "Lien_source",
        "Snippet_entreprise",
    ]

    if not results:
        df = pd.DataFrame(columns=final_columns)
    else:
        df = pd.DataFrame(results)

    for col in final_columns:
        if col not in df.columns:
            if col == "Source":
                df[col] = "Google / LinkedIn"
            elif col == "Statut":
                df[col] = "À contacter"
            elif col == "Priorité":
                df[col] = "Moyenne"
            else:
                df[col] = ""

    df = df[final_columns].fillna("")

    for col in ["Nom", "Poste", "Entreprise", "Ville", "Pays", "LinkedIn"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    if "LinkedIn" in df.columns:
        df = df.drop_duplicates(subset=["LinkedIn"])
    if "Nom" in df.columns and "Entreprise" in df.columns:
        df = df.drop_duplicates(subset=["Nom", "Entreprise"], keep="first")

    with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Prospects")
        wb = writer.book
        ws = writer.sheets["Prospects"]

        header_fill = PatternFill(fill_type="solid", fgColor="1F4E78")
        header_font = Font(color="FFFFFF", bold=True)
        thin_border = Border(
            left=Side(style="thin", color="D9D9D9"),
            right=Side(style="thin", color="D9D9D9"),
            top=Side(style="thin", color="D9D9D9"),
            bottom=Side(style="thin", color="D9D9D9"),
        )

        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = thin_border

        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.alignment = Alignment(vertical="top", wrap_text=True)
                cell.border = thin_border

        for col_idx in range(1, ws.max_column + 1):
            ws.column_dimensions[get_column_letter(col_idx)].width = 24

        ws.freeze_panes = "A2"

        last_row = ws.max_row
        last_col = ws.max_column
        if last_row >= 1 and last_col >= 1:
            table_ref = f"A1:{get_column_letter(last_col)}{last_row}"
            table = Table(displayName="TableProspects", ref=table_ref)
            style = TableStyleInfo(
                name="TableStyleMedium9",
                showFirstColumn=False,
                showLastColumn=False,
                showRowStripes=True,
                showColumnStripes=False,
            )
            table.tableStyleInfo = style
            ws.add_table(table)

        summary = wb.create_sheet("Résumé")
        summary["A1"] = "Recherche"
        summary["B1"] = keyword
        summary["A2"] = "Date export"
        summary["B2"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        summary["A3"] = "Nombre de lignes"
        summary["B3"] = len(df)

        for cell_ref in ["A1", "A2", "A3"]:
            summary[cell_ref].font = Font(bold=True)

        summary.column_dimensions["A"].width = 22
        summary.column_dimensions["B"].width = 42

    return file_name

# --------------------------------------------------
# RECHERCHE PAGINÉE GÉNÉRIQUE
# --------------------------------------------------
def google_search_page(
    query: str,
    custom_filters: Dict[str, str],
    start: int,
    page_size: int,
) -> Dict[str, object]:
    del custom_filters
    result = search_web(query=query, start=start, num=page_size)

    return {
        "organic_results": result.get("organic_results", []),
        "has_more": bool(result.get("has_more", False)),
        "next_start": result.get("next_start", start + page_size),
    }

# --------------------------------------------------
# RECHERCHE PROSPECTS
# --------------------------------------------------
def search_prospect_page(
    keyword: str,
    custom_filters: Optional[Dict[str, str]] = None,
    start: int = 0,
    page_size: int = SERP_BATCH_SIZE,
) -> Dict[str, object]:

    custom_filters = custom_filters or {}

    if len(keyword) > 300:
        keyword = keyword[:300]

    raw_keyword = clean_spaces(keyword)
    normalized_keyword = normalize_text(raw_keyword)

    # 🔥 mode offre
    if is_job_offer(raw_keyword):
        smart = smart_extract_search_query(raw_keyword)
        base_query = smart["job"]

        if smart["keywords"]:
            base_query += " " + " ".join(smart["keywords"])
        if smart["city"]:
            base_query += f" {smart['city']}"

        queries = [base_query]

        # variantes bonus pour mieux matcher
        if smart["keywords"]:
            queries.append(f"{smart['job']} {' '.join(smart['keywords'])}")
        if smart["city"]:
            queries.append(f"{smart['job']} {smart['city']}")

        params = {
            "job_titles": [smart["job"]],
            "keywords": smart["keywords"],
            "city": smart["city"],
            "experience": extract_job_params(raw_keyword).get("experience", 0),
        }

    else:
        # 🔥 mode mots-clés libres
        tokens = [t for t in normalized_keyword.split() if len(t) >= 2]

        queries = [raw_keyword]

        # ex: "programmeur fullstack react"
        if len(tokens) >= 2:
            queries.append(" ".join(tokens[:2]))
        if len(tokens) >= 3:
            queries.append(" ".join(tokens[:3]))

        # variantes ciblées si on détecte une techno / métier
        smart = smart_extract_search_query(raw_keyword)
        smart_query = smart["job"]
        if smart["keywords"]:
            smart_query += " " + " ".join(smart["keywords"])
        if smart["city"]:
            smart_query += f" {smart['city']}"

        if smart_query and normalize_text(smart_query) != normalized_keyword:
            queries.append(smart_query)

        queries = list(dict.fromkeys([q for q in queries if clean_spaces(q)]))

        params = {
            "job_titles": [smart.get("job") or raw_keyword],
            "keywords": smart.get("keywords", []),
            "city": smart.get("city", ""),
            "experience": 0,
        }

    all_results: List[Dict[str, str]] = []
    seen_links = set()

    for q in queries[:5]:
        query = build_prospect_query(q, custom_filters)
        page = google_search_page(query, custom_filters, start, page_size)
        organic_results = page["organic_results"]

        for item in organic_results:
            link = normalize_linkedin_url(item.get("link", ""))

            if not link or "linkedin.com/in/" not in link:
                continue
            if link in seen_links:
                continue

            seen_links.add(link)

            title_data = parse_google_title(item.get("title", ""))
            snippet = (item.get("snippet", "") or "").strip()

            row = {
                "Nom": title_data.get("Nom", ""),
                "Poste": title_data.get("Poste", ""),
                "Entreprise": title_data.get("Entreprise", ""),
                "Ville": custom_filters.get("ville", ""),
                "Pays": custom_filters.get("pays", ""),
                "Source": "Google / LinkedIn",
                "Statut": "À contacter",
                "Priorité": "Moyenne",
                "Notes": "",
                "Snippet": snippet,
                "LinkedIn": link,
            }

            # 🔥 filtrage intelligent
            if is_job_offer(raw_keyword):
                if not row_matches_job_offer(row, params):
                    continue
            else:
                # on accepte soit le match alias classique,
                # soit un match sur les tokens du texte
                token_match = False
                for token in [t for t in normalized_keyword.split() if len(t) >= 3]:
                    haystack = normalize_text(
                        f"{row.get('Poste', '')} {row.get('Entreprise', '')} {row.get('Snippet', '')}"
                    )
                    if token in haystack:
                        token_match = True
                        break

                if not row_matches_keyword(row, raw_keyword) and not token_match:
                    continue

            row["MatchScore"] = score_profile_advanced(row, params, custom_filters)
            all_results.append(row)

    all_results.sort(key=lambda x: x.get("MatchScore", 0), reverse=True)

    return {
        "results": all_results[:page_size],
        "next_start": start + page_size,
        "has_more": len(all_results) > page_size,
    }
# --------------------------------------------------
# RECHERCHE PERSONNE LINKEDIN
# --------------------------------------------------
def search_person_page(
    person_name: str,
    custom_filters: Optional[Dict[str, str]] = None,
    start: int = 0,
    page_size: int = SERP_BATCH_SIZE,
    fuzzy_enabled: bool = False,
) -> Dict[str, object]:
    custom_filters = custom_filters or {}
    cache_key = make_cache_key("person", person_name, custom_filters, start, page_size, fuzzy_enabled)

    cached = get_cache_payload(cache_key)
    if cached:
        return cached

    query = build_person_query(person_name, custom_filters, fuzzy_enabled)
    page = google_search_page(query, custom_filters, start, page_size)
    organic_results = page["organic_results"]

    results: List[Dict[str, str]] = []
    seen_links = set()

    for item in organic_results:
        link = normalize_linkedin_url(item.get("link", ""))
        if not link or "linkedin.com/in/" not in link:
            continue
        if link in seen_links:
            continue
        seen_links.add(link)

        title_data = parse_google_title(item.get("title", ""))
        snippet = (item.get("snippet", "") or "").strip()

        row = {
            "Nom": title_data.get("Nom", ""),
            "Poste": title_data.get("Poste", ""),
            "Entreprise": title_data.get("Entreprise", ""),
            "Ville": custom_filters.get("ville", ""),
            "Pays": custom_filters.get("pays", ""),
            "Source": "Google / LinkedIn",
            "Statut": "À contacter",
            "Priorité": "Moyenne",
            "Notes": "",
            "Snippet": snippet,
            "LinkedIn": link,
        }

        if not row_matches_person(row, person_name, fuzzy_enabled):
            continue

        results.append(row)

        if start + len(results) >= MAX_RESULTS:
            break

    results.sort(key=lambda x: float(x.get("MatchScore", 0) or 0), reverse=True)

    payload = {
        "results": results,
        "next_start": page["next_start"],
        "has_more": bool(page["has_more"]) and (start + len(results) < MAX_RESULTS),
    }

    set_cache_payload(cache_key, payload)
    return payload

# --------------------------------------------------
# EXPORT COMPLET
# --------------------------------------------------
def search_full_export(
    search_mode: str,
    base_value: str,
    custom_filters: Optional[Dict[str, str]] = None,
    max_results: int = MAX_RESULTS,
    fuzzy_enabled: bool = False,
) -> List[Dict[str, str]]:
    custom_filters = custom_filters or {}
    all_results: List[Dict[str, str]] = []
    seen = set()
    start = 0

    while len(all_results) < max_results:
        if search_mode == "person":
            page = search_person_page(
                person_name=base_value,
                custom_filters=custom_filters,
                start=start,
                page_size=SERP_BATCH_SIZE,
                fuzzy_enabled=fuzzy_enabled,
            )
        else:
            page = search_prospect_page(
                keyword=base_value,
                custom_filters=custom_filters,
                start=start,
                page_size=SERP_BATCH_SIZE,
            )

        page_results = page.get("results", [])
        if not page_results:
            break

        for row in page_results:
            link = row.get("LinkedIn", "")
            if link and link not in seen:
                seen.add(link)
                all_results.append(row)
                if len(all_results) >= max_results:
                    break

        if not page.get("has_more", False):
            break

        start = page.get("next_start", start + SERP_BATCH_SIZE)

    return all_results

# --------------------------------------------------
# AFFICHAGE / PRÉCHARGEMENT
# --------------------------------------------------
async def fetch_display_chunk(
    search_mode: str,
    base_value: str,
    custom_filters: Dict[str, str],
    start: int,
    target_count: int = DISPLAY_PAGE_SIZE,
    fuzzy_enabled: bool = False,
) -> Dict[str, object]:
    collected: List[Dict[str, str]] = []
    current_start = start
    has_more_remote = True
    seen_links = set()

    while len(collected) < target_count and has_more_remote and len(collected) < MAX_RESULTS:
        if search_mode == "person":
            page = await asyncio.to_thread(
                search_person_page,
                base_value,
                custom_filters,
                current_start,
                SERP_BATCH_SIZE,
                fuzzy_enabled,
            )
        else:
            page = await asyncio.to_thread(
                search_prospect_page,
                base_value,
                custom_filters,
                current_start,
                SERP_BATCH_SIZE,
            )

        page_results = page.get("results", [])
        for row in page_results:
            link = row.get("LinkedIn", "")
            if link and link not in seen_links:
                seen_links.add(link)
                collected.append(row)
                if len(collected) >= target_count:
                    break

        has_more_remote = bool(page.get("has_more", False))
        current_start = page.get("next_start", current_start + SERP_BATCH_SIZE)

        if not page_results:
            break

    return {
        "results": collected[:target_count],
        "next_start": current_start,
        "has_more": has_more_remote,
    }


def build_result_blocks(results: List[Dict[str, str]], start_index: int, page_size: int = DISPLAY_PAGE_SIZE) -> List[str]:
    subset = results[start_index:start_index + page_size]
    blocks = []

    for idx, r in enumerate(subset, start=start_index + 1):
        nom = esc(r.get("Nom") or "Nom inconnu")
        poste = esc(r.get("Poste") or "N/A")
        entreprise = esc(r.get("Entreprise") or "N/A")
        lien = esc(r.get("LinkedIn") or "N/A")
        match_score = r.get("MatchScore", "")

        match_line = f"\n• <b>Match :</b> {esc(match_score)}" if str(match_score).strip() else ""

        block = (
            f"<b>{idx}. {nom}</b>\n"
            f"• <b>Poste :</b> {poste}\n"
            f"• <b>Entreprise :</b> {entreprise}"
            f"{match_line}\n"
            f"• <b>LinkedIn :</b> {lien}"
        )
        blocks.append(block)

    return blocks


def ensure_prefetch_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.setdefault("results", [])
    context.user_data.setdefault("index", 0)
    context.user_data.setdefault("next_start", 0)
    context.user_data.setdefault("has_more_remote", True)
    context.user_data.setdefault("prefetched_chunk", None)
    context.user_data.setdefault("prefetch_task", None)
    context.user_data.setdefault("page_lock", asyncio.Lock())


async def prefetch_next_chunk(context: ContextTypes.DEFAULT_TYPE) -> None:
    ensure_prefetch_state(context)

    if not context.user_data.get("has_more_remote", True):
        return

    if context.user_data.get("prefetched_chunk") is not None:
        return

    search_mode = context.user_data.get("search_mode")
    base_value = context.user_data.get("base_value")
    custom_filters = context.user_data.get("filters", {})
    start = context.user_data.get("next_start", 0)
    fuzzy_enabled = bool(context.user_data.get("fuzzy_enabled", False))

    if not search_mode or not base_value:
        return

    chunk = await fetch_display_chunk(
        search_mode=search_mode,
        base_value=base_value,
        custom_filters=custom_filters,
        start=start,
        target_count=DISPLAY_PAGE_SIZE,
        fuzzy_enabled=fuzzy_enabled,
    )

    context.user_data["prefetched_chunk"] = chunk
    context.user_data["has_more_remote"] = bool(chunk.get("has_more", False))


def start_prefetch(context: ContextTypes.DEFAULT_TYPE) -> None:
    ensure_prefetch_state(context)

    existing_task = context.user_data.get("prefetch_task")
    if existing_task and not existing_task.done():
        return

    if context.user_data.get("prefetched_chunk") is not None:
        return

    if not context.user_data.get("has_more_remote", True):
        return

    task = context.application.create_task(prefetch_next_chunk(context))
    context.user_data["prefetch_task"] = task


async def wait_for_prefetch_if_needed(context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict[str, object]]:
    ensure_prefetch_state(context)

    cached_chunk = context.user_data.get("prefetched_chunk")
    if cached_chunk is not None:
        context.user_data["prefetched_chunk"] = None
        return cached_chunk

    task = context.user_data.get("prefetch_task")
    if task and not task.done():
        await task

    cached_chunk = context.user_data.get("prefetched_chunk")
    if cached_chunk is not None:
        context.user_data["prefetched_chunk"] = None
        return cached_chunk

    if context.user_data.get("has_more_remote", True):
        chunk = await fetch_display_chunk(
            search_mode=context.user_data.get("search_mode"),
            base_value=context.user_data.get("base_value"),
            custom_filters=context.user_data.get("filters", {}),
            start=context.user_data.get("next_start", 0),
            target_count=DISPLAY_PAGE_SIZE,
            fuzzy_enabled=bool(context.user_data.get("fuzzy_enabled", False)),
        )
        context.user_data["has_more_remote"] = bool(chunk.get("has_more", False))
        return chunk

    return None


async def send_next_page(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    ensure_prefetch_state(context)

    results = context.user_data.get("results", [])
    index = context.user_data.get("index", 0)

    if index >= len(results):
        chunk = await wait_for_prefetch_if_needed(context)

        if not chunk or not chunk.get("results"):
            await context.bot.send_message(
                chat_id=chat_id,
                text="Plus de résultats disponibles.",
                parse_mode=ParseMode.HTML,
            )
            return

        chunk_results = chunk["results"]
        context.user_data["results"].extend(chunk_results)
        context.user_data["next_start"] = chunk["next_start"]
        context.user_data["has_more_remote"] = bool(chunk["has_more"])

    results = context.user_data.get("results", [])
    index = context.user_data.get("index", 0)

    blocks = build_result_blocks(results, index, DISPLAY_PAGE_SIZE)
    if not blocks:
        await context.bot.send_message(
            chat_id=chat_id,
            text="Plus de résultats disponibles.",
            parse_mode=ParseMode.HTML,
        )
        return

    full_text = "\n\n".join(blocks)
    chunks = split_long_message(full_text)

    new_index = min(index + DISPLAY_PAGE_SIZE, len(results))
    context.user_data["index"] = new_index

    has_local_more = new_index < len(results)
    has_remote_more = context.user_data.get("has_more_remote", False)
    show_button = has_local_more or has_remote_more
    markup = pagination_keyboard() if show_button else None

    for i, chunk_text in enumerate(chunks):
        await context.bot.send_message(
            chat_id=chat_id,
            text=chunk_text,
            reply_markup=markup if i == len(chunks) - 1 else None,
            disable_web_page_preview=True,
            parse_mode=ParseMode.HTML,
        )

    if context.user_data.get("index", 0) >= len(context.user_data.get("results", [])):
        start_prefetch(context)


async def render_company_page(message_obj, context: ContextTypes.DEFAULT_TYPE) -> None:
    companies = context.user_data.get("company_results", [])
    current_page = safe_int(context.user_data.get("company_page", 0))

    if not companies:
        text = "Plus de résultats disponibles."
        markup = None
    else:
        total_pages = (len(companies) + COMPANY_PAGE_SIZE - 1) // COMPANY_PAGE_SIZE
        current_page = max(0, min(current_page, max(total_pages - 1, 0)))
        context.user_data["company_page"] = current_page

        text = build_company_page_text(companies, current_page)
        markup = company_page_keyboard(current_page, len(companies))

    await safe_edit_message_text(
        message_obj=message_obj,
        text=text,
        reply_markup=markup,
        disable_web_page_preview=True,
        parse_mode=ParseMode.HTML,
    )

# --------------------------------------------------
# ÉTAT
# --------------------------------------------------
def reset_user_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["flow"] = "idle"
    context.user_data["search_mode"] = None
    context.user_data["base_value"] = None
    context.user_data["keyword"] = None
    context.user_data["person_name"] = None
    context.user_data["fuzzy_enabled"] = False
    context.user_data["filters"] = {}
    context.user_data["results"] = []
    context.user_data["index"] = 0
    context.user_data["next_start"] = 0
    context.user_data["has_more_remote"] = True
    context.user_data["prefetched_chunk"] = None
    context.user_data["prefetch_task"] = None
    context.user_data["page_lock"] = asyncio.Lock()
    context.user_data["company_results"] = []
    context.user_data["company_page"] = 0
    context.user_data["company_list_message_id"] = None

# --------------------------------------------------
# ENVOI BRANDING
# --------------------------------------------------
async def send_brand_welcome(message_obj) -> None:
    caption = f"{brand_header_text()}\n\n{main_menu_text()}"

    if BRAND_IMAGE:
        try:
            if os.path.exists(BRAND_IMAGE):
                with open(BRAND_IMAGE, "rb") as img:
                    await message_obj.reply_photo(
                        photo=img,
                        caption=caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=menu_keyboard(),
                    )
            else:
                await message_obj.reply_photo(
                    photo=BRAND_IMAGE,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=menu_keyboard(),
                )
            return
        except Exception as e:
            logger.warning("Impossible d'envoyer la bannière: %s", e)

    await message_obj.reply_text(
        caption,
        parse_mode=ParseMode.HTML,
        reply_markup=menu_keyboard(),
        disable_web_page_preview=True,
    )

# --------------------------------------------------
# COMMANDES
# --------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reset_user_flow(context)
    if update.message:
        await send_brand_welcome(update.message)


async def prospects(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reset_user_flow(context)
    context.user_data["flow"] = "awaiting_keyword"
    context.user_data["search_mode"] = "prospect"
    await update.message.reply_text(
        prospects_intro_text(),
        parse_mode=ParseMode.HTML,
    )


async def person_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reset_user_flow(context)
    await update.message.reply_text(
        person_intro_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=person_menu_keyboard(),
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reset_user_flow(context)
    await update.message.reply_text(
        "❌ <b>Recherche annulée.</b>\nTu peux relancer avec /start.",
        parse_mode=ParseMode.HTML,
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "<b>Commandes disponibles</b>\n\n"
        "/start\n"
        "/help\n"
        "/prospects\n"
        "/personne\n"
        "/cancel\n\n"
        "<b>Modules</b>\n"
        "• Recherche prospects LinkedIn\n"
        "• Recherche personne sur LinkedIn\n"
        "• Recherche entreprise\n\n"
        "<b>Exemples recherche entreprise</b>\n"
        "<code>Dupont</code>\n"
        "<code>Dupont Martin</code>\n"
        "<code>Dupont Martin,ville=Paris</code>",
        parse_mode=ParseMode.HTML,
    )

# --------------------------------------------------
# TEXTE
# --------------------------------------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    flow = context.user_data.get("flow", "idle")
    text = (update.message.text or "").strip()

    if flow == "awaiting_keyword":
        if not text:
            await update.message.reply_text(
                "Merci d'envoyer un mot-clé valide.",
                parse_mode=ParseMode.HTML,
            )
            return

        context.user_data["keyword"] = text
        context.user_data["base_value"] = text
        context.user_data["flow"] = "awaiting_filters"

        await update.message.reply_text(
            filters_help_text(),
            parse_mode=ParseMode.HTML,
        )
        return

    if flow == "awaiting_filters":
        context.user_data["filters"] = parse_filters(text)
        context.user_data["flow"] = "awaiting_excel_choice"

        await update.message.reply_text(
            excel_choice_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=excel_keyboard(),
        )
        return

    if flow == "awaiting_person_name":
        if not text:
            await update.message.reply_text(
                "Merci d'envoyer un nom et prénom valides.",
                parse_mode=ParseMode.HTML,
            )
            return

        context.user_data["person_name"] = text
        context.user_data["base_value"] = text
        context.user_data["flow"] = "awaiting_person_fuzzy"

        await update.message.reply_text(
            "<b>Orthographe proche</b>\n\n"
            "Souhaites-tu prendre en compte une orthographe approchante ?\n"
            "Exemple : Sarah / Sarra / Sara",
            parse_mode=ParseMode.HTML,
            reply_markup=fuzzy_keyboard(),
        )
        return

    if flow == "awaiting_person_filters":
        context.user_data["filters"] = parse_filters(text)
        context.user_data["flow"] = "awaiting_person_excel_choice"

        await update.message.reply_text(
            excel_choice_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=excel_keyboard(),
        )
        return

    if flow == "awaiting_company_query":
        name, company_filters = parse_company_query_input(text)

        if not name:
            await update.message.reply_text(
                "Merci d'envoyer un nom ou un nom prénom valide.\n"
                "Exemple : <code>Dupont,ville=Paris</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        ville_filter = company_filters.get("ville", "")

        await update.message.reply_text(
            company_search_loading_text(),
            parse_mode=ParseMode.HTML,
        )

        companies = await asyncio.to_thread(search_company_person, name, ville_filter)

        if not companies:
            await update.message.reply_text(
                no_result_text(),
                parse_mode=ParseMode.HTML,
            )
            reset_user_flow(context)
            return

        context.user_data["company_results"] = companies
        context.user_data["company_page"] = 0
        context.user_data["flow"] = "company_results_ready"

        await update.message.reply_text(
            search_done_text(len(companies), "company"),
            parse_mode=ParseMode.HTML,
        )

        page_message = await update.message.reply_text(
            "Chargement des résultats…",
            parse_mode=ParseMode.HTML,
        )
        context.user_data["company_list_message_id"] = page_message.message_id
        await render_company_page(page_message, context)
        return

    await update.message.reply_text(
        "Utilise /start, /prospects ou /personne pour démarrer.",
        parse_mode=ParseMode.HTML,
    )

# --------------------------------------------------
# CALLBACKS
# --------------------------------------------------
async def launch_search(query, context: ContextTypes.DEFAULT_TYPE, export_excel_requested: bool) -> None:
    search_mode = context.user_data.get("search_mode")
    base_value = context.user_data.get("base_value")
    custom_filters = context.user_data.get("filters", {})
    fuzzy_enabled = bool(context.user_data.get("fuzzy_enabled", False))

    if not search_mode or not base_value:
        await query.message.reply_text(
            "Recherche introuvable. Relance avec /start.",
            parse_mode=ParseMode.HTML,
        )
        reset_user_flow(context)
        return

    context.user_data["flow"] = "searching"
    await context.bot.send_chat_action(chat_id=query.message.chat_id, action=ChatAction.TYPING)

    await query.message.reply_text(
        loading_search_text(search_mode),
        parse_mode=ParseMode.HTML,
    )

    try:
        first_chunk = await fetch_display_chunk(
            search_mode=search_mode,
            base_value=base_value,
            custom_filters=custom_filters,
            start=0,
            target_count=DISPLAY_PAGE_SIZE,
            fuzzy_enabled=fuzzy_enabled,
        )
    except Exception as e:
        logger.exception("Erreur pendant la recherche initiale")
        await query.message.reply_text(
            f"Erreur pendant la recherche : <code>{esc(e)}</code>",
            parse_mode=ParseMode.HTML,
        )
        reset_user_flow(context)
        return

    first_results = first_chunk.get("results", [])
    if not first_results:
        await query.message.reply_text(
            no_result_text(),
            parse_mode=ParseMode.HTML,
        )
        reset_user_flow(context)
        return

    context.user_data["results"] = first_results[:]
    context.user_data["index"] = 0
    context.user_data["next_start"] = first_chunk["next_start"]
    context.user_data["has_more_remote"] = bool(first_chunk["has_more"])
    context.user_data["prefetched_chunk"] = None
    context.user_data["prefetch_task"] = None
    context.user_data["flow"] = "results_ready"

    await query.message.reply_text(
        search_done_text(len(first_results), search_mode),
        parse_mode=ParseMode.HTML,
    )
    await send_next_page(query.message.chat_id, context)
    start_prefetch(context)

    if export_excel_requested:
        await query.message.reply_text(
            "📊 <b>Génération de l’Excel complet…</b>",
            parse_mode=ParseMode.HTML,
        )
        try:
            full_results = await asyncio.to_thread(
                search_full_export,
                search_mode,
                base_value,
                custom_filters,
                MAX_RESULTS,
                fuzzy_enabled,
            )
            file_path = export_excel(full_results, base_value)
            with open(file_path, "rb") as f:
                await query.message.reply_document(
                    document=f,
                    filename=os.path.basename(file_path),
                    caption=f"Voici ton export Excel complet ({len(full_results)} profils).",
                )
        except Exception as e:
            logger.exception("Erreur export Excel complet")
            await query.message.reply_text(
                f"Impossible de générer l’Excel : <code>{esc(e)}</code>",
                parse_mode=ParseMode.HTML,
            )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return

    await query.answer()
    data = query.data
    flow = context.user_data.get("flow", "idle")

    if data == "menu_prospects":
        reset_user_flow(context)
        context.user_data["flow"] = "awaiting_keyword"
        context.user_data["search_mode"] = "prospect"
        await query.message.reply_text(
            prospects_intro_text(),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "menu_person_menu":
        reset_user_flow(context)
        await query.message.reply_text(
            person_intro_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=person_menu_keyboard(),
        )
        return

    if data == "person_linkedin":
        reset_user_flow(context)
        context.user_data["flow"] = "awaiting_person_name"
        context.user_data["search_mode"] = "person"
        await query.message.reply_text(
            "<b>Recherche LinkedIn</b>\n\nQuel nom et prénom veux-tu rechercher ?",
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "person_company":
        reset_user_flow(context)
        context.user_data["flow"] = "awaiting_company_query"
        context.user_data["search_mode"] = "company"
        await query.message.reply_text(
            "<b>Recherche entreprise</b>\n\n"
            "Entre un nom ou un nom prénom.\n"
            "Tu peux ajouter une ville si tu veux.\n\n"
            "Exemple : <code>Dupont,ville=Paris</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "more":
        lock = context.user_data.get("page_lock")
        if lock is None:
            context.user_data["page_lock"] = asyncio.Lock()
            lock = context.user_data["page_lock"]

        async with lock:
            await send_next_page(query.message.chat_id, context)
        return

    if data == "company_next":
        if context.user_data.get("flow") != "company_results_ready":
            await query.message.reply_text(
                "Cette action n'est plus valide. Relance avec /personne.",
                parse_mode=ParseMode.HTML,
            )
            return

        companies = context.user_data.get("company_results", [])
        total_pages = max(1, (len(companies) + COMPANY_PAGE_SIZE - 1) // COMPANY_PAGE_SIZE)
        current_page = safe_int(context.user_data.get("company_page", 0))

        if current_page >= total_pages - 1:
            return

        context.user_data["company_page"] = current_page + 1
        await render_company_page(query.message, context)
        return

    if data == "company_prev":
        if context.user_data.get("flow") != "company_results_ready":
            await query.message.reply_text(
                "Cette action n'est plus valide. Relance avec /personne.",
                parse_mode=ParseMode.HTML,
            )
            return

        current_page = safe_int(context.user_data.get("company_page", 0))
        if current_page <= 0:
            return

        context.user_data["company_page"] = current_page - 1
        await render_company_page(query.message, context)
        return

    if data == "company_back":
        if context.user_data.get("flow") != "company_results_ready":
            await query.message.reply_text(
                "Cette action n'est plus valide. Relance avec /personne.",
                parse_mode=ParseMode.HTML,
            )
            return

        try:
            await query.message.delete()
        except BadRequest:
            pass
        return

    if data.startswith("company_detail_"):
        try:
            idx = int(data.split("_")[-1])
        except Exception:
            await query.message.reply_text(
                "Impossible d’ouvrir le détail de ce résultat.",
                parse_mode=ParseMode.HTML,
            )
            return

        company_results = context.user_data.get("company_results", [])
        if idx < 0 or idx >= len(company_results):
            await query.message.reply_text(
                "Ce résultat n’est plus disponible. Relance la recherche.",
                parse_mode=ParseMode.HTML,
            )
            return

        row = company_results[idx]
        detail_text = build_company_detail_text(idx + 1, row)

        await query.message.reply_text(
            detail_text,
            reply_markup=company_detail_keyboard(),
            disable_web_page_preview=True,
            parse_mode=ParseMode.HTML,
        )
        return

    if data in {"fuzzy_yes", "fuzzy_no"}:
        if flow != "awaiting_person_fuzzy":
            await query.message.reply_text(
                "Cette action n'est plus valide. Relance avec /personne.",
                parse_mode=ParseMode.HTML,
            )
            return

        context.user_data["fuzzy_enabled"] = (data == "fuzzy_yes")
        context.user_data["flow"] = "awaiting_person_filters"

        await query.message.reply_text(
            person_filters_help_text(),
            parse_mode=ParseMode.HTML,
        )
        return

    if data in {"excel_yes", "excel_no"}:
        export_excel_requested = data == "excel_yes"

        if flow == "awaiting_excel_choice":
            await launch_search(query, context, export_excel_requested)
            return

        if flow == "awaiting_person_excel_choice":
            await launch_search(query, context, export_excel_requested)
            return

        await query.message.reply_text(
            "Cette action n'est plus valide. Relance avec /start.",
            parse_mode=ParseMode.HTML,
        )
        return

# --------------------------------------------------
# ERREURS
# --------------------------------------------------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Erreur non gérée", exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="⚠️ Une erreur inattendue s'est produite. Réessaie avec /start.",
                parse_mode=ParseMode.HTML,
            )
    except Exception:
        pass

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main() -> None:
    load_cache()

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("prospects", prospects))
    app.add_handler(CommandHandler("personne", person_search))
    app.add_handler(CommandHandler("cancel", cancel))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.add_error_handler(error_handler)

    print(f"{BOT_BRAND_NAME} actif")
    app.run_polling()


if __name__ == "__main__":
    main()
