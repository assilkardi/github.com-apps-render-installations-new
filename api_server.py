from typing import Any, Dict, List, Literal, Optional
import asyncio
import os

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from main import (
    ACCESS_STATE,
    PROVIDER_STATS,
    SEARCH_CACHE,
    export_excel,
    provider_cooldown_remaining,
    search_company_person,
    search_full_export,
)


app = FastAPI(title="LeadGen Premium API")

raw_origins = os.getenv("API_ALLOW_ORIGINS", "*").strip()
allow_origins = ["*"] if raw_origins == "*" else [o.strip() for o in raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXPORT_DIR = os.path.join(BASE_DIR, "exports")
MAX_API_RESULTS = 200
MAX_QUERY_LENGTH = 300
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()


class SearchRequest(BaseModel):
    mode: Literal["prospect", "person", "company"]
    query: str = Field(..., min_length=1, max_length=MAX_QUERY_LENGTH)
    filters: Optional[Dict[str, str]] = None
    fuzzy_enabled: bool = False
    export_excel_requested: bool = False
    max_results: int = Field(default=20, ge=1, le=MAX_API_RESULTS)


class SearchResponse(BaseModel):
    success: bool
    mode: str
    count: int
    results: List[Dict[str, Any]]
    excel_file: Optional[str] = None
    message: Optional[str] = None
    
# --------------------------------------------------
# COMPANY RESULT NORMALIZATION + SCORING
# --------------------------------------------------

def normalize_company_result(item):
    return {
        "Dirigeant": item.get("Dirigeant") or item.get("nom_complet") or item.get("dirigeant") or "",
        "Entreprise_INPI": item.get("Entreprise_INPI") or item.get("Entreprise") or item.get("raison_sociale") or "",
        "SIREN": item.get("SIREN") or item.get("siren") or "",
        "Adresse_INPI": item.get("Adresse_INPI") or item.get("Adresse") or item.get("adresse") or item.get("adresse_complete") or "",
        "Ville_INPI": item.get("Ville_INPI") or item.get("Ville") or item.get("ville") or item.get("libelle_commune") or "",
        "Activite": item.get("Activite") or item.get("activite") or item.get("code_naf") or "",
        "Date_creation": item.get("Date_creation") or item.get("date_creation") or "",
        "Lien_source": item.get("Lien_source") or item.get("source_url") or item.get("lien_source") or "",
        "Source": item.get("Source") or item.get("source") or "",
    }


def company_result_score(item):
    score = 0

    if item.get("Dirigeant"):
        score += 50
    if item.get("Entreprise_INPI"):
        score += 40
    if item.get("Adresse_INPI"):
        score += 30
    if item.get("Ville_INPI"):
        score += 20
    if item.get("Lien_source"):
        score += 20
    if item.get("Activite"):
        score += 10
    if item.get("Date_creation"):
        score += 5

    return score


def check_admin_token(x_admin_token: Optional[str]) -> None:
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN non configuré côté serveur.")

    if not x_admin_token or x_admin_token.strip() != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Accès admin refusé.")


@app.get("/")
def root():
    return {
        "ok": True,
        "message": "LeadGen Premium API opérationnelle."
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "LeadGen Premium API",
        "cache_entries": len(SEARCH_CACHE) if isinstance(SEARCH_CACHE, dict) else 0,
        "approved_users": len(ACCESS_STATE.get("approved_users", {})),
        "pending_users": len(ACCESS_STATE.get("pending_users", {})),
        "blacklist": len(ACCESS_STATE.get("blacklist", {})),
        "provider_stats": PROVIDER_STATS,
        "serpapi_cooldown_seconds": provider_cooldown_remaining("serpapi"),
        "serper_cooldown_seconds": provider_cooldown_remaining("serper"),
        "cors": allow_origins,
    }


@app.get("/admin/stats")
def admin_stats(x_admin_token: Optional[str] = Header(default=None)):
    check_admin_token(x_admin_token)

    return {
        "ok": True,
        "service": "LeadGen Premium API",
        "cache_entries": len(SEARCH_CACHE) if isinstance(SEARCH_CACHE, dict) else 0,
        "approved_users": len(ACCESS_STATE.get("approved_users", {})),
        "pending_users": len(ACCESS_STATE.get("pending_users", {})),
        "blacklist": len(ACCESS_STATE.get("blacklist", {})),
        "provider_stats": PROVIDER_STATS,
        "serpapi_cooldown_seconds": provider_cooldown_remaining("serpapi"),
        "serper_cooldown_seconds": provider_cooldown_remaining("serper"),
        "cors": allow_origins,
    }


@app.get("/download/{filename}")
def download_file(filename: str):
    safe_name = os.path.basename(filename)
    file_path = os.path.join(EXPORT_DIR, safe_name)

    if not os.path.exists(file_path):
        return {"success": False, "message": "Fichier introuvable."}

    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=safe_name,
    )


@app.post("/search", response_model=SearchResponse)
async def search_endpoint(payload: SearchRequest):
    mode = payload.mode
    query = payload.query.strip()
    filters = {k: str(v) for k, v in (payload.filters or {}).items() if str(v).strip()}

    try:
        if mode == "company":
            ville_filter = filters.get("ville", "")
            results = await asyncio.to_thread(search_company_person, query, ville_filter)
            results = [normalize_company_result(r) for r in results]
            seen = set()
            deduped = []
            for r in results:
                key = (
                r.get("Dirigeant", "").strip().lower(),
                r.get("Entreprise_INPI", "").strip().lower(),
                r.get("SIREN", "").strip().lower(),
        )
            if key not in seen:
                seen.add(key)
                deduped.append(r)
            results = sorted(
            deduped,
            key=lambda x: (
                company_result_score(x),
                x.get("Dirigeant", "").strip().lower(),
                x.get("Entreprise_INPI", "").strip().lower(),
            ),
            reverse=True,
        )

            print("DEBUG COMPANY RESULTS =", results[:3])
            
            excel_file = None
            if payload.export_excel_requested and results:
                file_path = await asyncio.to_thread(export_excel, results, query)
                excel_file = os.path.basename(file_path)

            return SearchResponse(
                success=True,
                mode=mode,
                count=len(results),
                results=results[: payload.max_results],
                excel_file=excel_file,
                message="Recherche entreprise terminée.",
            )

        results = await asyncio.to_thread(
            search_full_export,
            mode,
            query,
            filters,
            payload.max_results,
            payload.fuzzy_enabled,
        )

        excel_file = None
        if payload.export_excel_requested and results:
            file_path = await asyncio.to_thread(export_excel, results, query)
            excel_file = os.path.basename(file_path)

        return SearchResponse(
            success=True,
            mode=mode,
            count=len(results),
            results=results[: payload.max_results],
            excel_file=excel_file,
            message="Recherche terminée.",
        )

    except Exception as e:
        error_message = str(e)
        if "429" in error_message or "Too Many Requests" in error_message:
            error_message = "Le service de recherche est temporairement saturé. Réessaie dans quelques minutes."

        return SearchResponse(
            success=False,
            mode=mode,
            count=0,
            results=[],
            excel_file=None,
            message=error_message,
        )