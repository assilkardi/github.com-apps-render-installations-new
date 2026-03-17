from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Literal, Any
import asyncio
import os

from main import (
    search_full_export,
    search_company_person,
    export_excel,
)

app = FastAPI(title="LeadGen Premium API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXPORT_DIR = os.path.join(BASE_DIR, "exports")


class SearchRequest(BaseModel):
    mode: Literal["prospect", "person", "company"]
    query: str = Field(..., min_length=1)
    filters: Optional[Dict[str, str]] = None
    fuzzy_enabled: bool = False
    export_excel_requested: bool = False
    max_results: int = 20


class SearchResponse(BaseModel):
    success: bool
    mode: str
    count: int
    results: List[Dict[str, Any]]
    excel_file: Optional[str] = None
    message: Optional[str] = None


@app.get("/health")
def health():
    return {"ok": True, "service": "LeadGen Premium API"}


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
    filters = payload.filters or {}

    try:
        if mode == "company":
            ville_filter = filters.get("ville", "")
            results = await asyncio.to_thread(search_company_person, query, ville_filter)

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