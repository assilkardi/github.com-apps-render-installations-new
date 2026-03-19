from __future__ import annotations

import asyncio
import html
import logging
from pathlib import Path
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.core.config import settings
from app.core.logging_config import configure_logging
from app.core.utils import parse_filters
from app.services.company_service import search_company_person
from app.services.export_service import export_excel
from app.services.search_service import search_person, search_prospects

configure_logging()
logger = logging.getLogger(__name__)

BOT_NAME = "LeadGen Premium Ultra"
SESSION_PAGE_SIZE = 4
MAX_RESULTS_OPTIONS = [10, 20, 50, 100]
MAX_RESULTS_DEFAULT = 20


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def esc(value: Any) -> str:
    return html.escape(str(value or ""))


def reset_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    preserved = {
        "results": context.user_data.get("results"),
        "mode": context.user_data.get("mode"),
        "query": context.user_data.get("query"),
        "filters": context.user_data.get("filters"),
        "max_results": context.user_data.get("max_results", MAX_RESULTS_DEFAULT),
        "export": context.user_data.get("export", False),
        "page": context.user_data.get("page", 0),
        "last_summary": context.user_data.get("last_summary", ""),
    }
    context.user_data.clear()
    context.user_data.update({k: v for k, v in preserved.items() if v is not None})


def clear_search_session(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in [
        "results",
        "mode",
        "query",
        "filters",
        "max_results",
        "export",
        "page",
        "last_summary",
        "details_index",
        "results_message_id",
    ]:
        context.user_data.pop(key, None)
    context.user_data["flow"] = "idle"


def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🚀 Prospects LinkedIn", callback_data="mode:prospect")],
            [InlineKeyboardButton("👤 Recherche personne", callback_data="mode:person")],
            [InlineKeyboardButton("🏢 Entreprise / dirigeant", callback_data="mode:company")],
            [
                InlineKeyboardButton("🧠 Aide", callback_data="menu:help"),
                InlineKeyboardButton("📊 Dernière recherche", callback_data="menu:last"),
            ],
        ]
    )


def back_to_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Retour menu", callback_data="menu:home")]])


def export_choice_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📊 Oui, avec Excel", callback_data="export:yes")],
            [InlineKeyboardButton("➡️ Non, seulement les résultats", callback_data="export:no")],
            [InlineKeyboardButton("⬅️ Retour menu", callback_data="menu:home")],
        ]
    )


def max_results_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    current_row: list[InlineKeyboardButton] = []
    for value in MAX_RESULTS_OPTIONS:
        current_row.append(InlineKeyboardButton(f"{value}", callback_data=f"max:{value}"))
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    rows.append([InlineKeyboardButton("⬅️ Retour menu", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def results_keyboard(page: int, total_pages: int, index_base: int, page_count: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    detail_buttons = [
        InlineKeyboardButton(f"Détail {index_base + offset + 1}", callback_data=f"detail:{index_base + offset}")
        for offset in range(page_count)
    ]
    for i in range(0, len(detail_buttons), 2):
        rows.append(detail_buttons[i : i + 2])

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Précédent", callback_data="page:prev"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Suivant ➡️", callback_data="page:next"))
    if nav_row:
        rows.append(nav_row)

    rows.append(
        [
            InlineKeyboardButton("📊 Export Excel", callback_data="action:export"),
            InlineKeyboardButton("🏠 Menu", callback_data="menu:home"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def detail_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔙 Retour résultats", callback_data="action:back_results")],
            [InlineKeyboardButton("📊 Export Excel", callback_data="action:export")],
        ]
    )


def mode_label(mode: str) -> str:
    return {
        "prospect": "Prospects LinkedIn",
        "person": "Recherche personne",
        "company": "Entreprise / dirigeant",
    }.get(mode, "Recherche")


def mode_examples(mode: str) -> str:
    if mode == "prospect":
        return (
            "Exemples :\n"
            "• <code>business developer</code>\n"
            "• <code>responsable marketing</code>\n"
            "• <code>recruteur</code>"
        )
    if mode == "person":
        return (
            "Exemples :\n"
            "• <code>Jean Dupont</code>\n"
            "• <code>Sarah Ben Ali</code>"
        )
    return (
        "Exemples :\n"
        "• <code>Dupont</code>\n"
        "• <code>Airbus</code>\n"
        "• <code>Bolloré,ville=Paris</code>"
    )


def help_text() -> str:
    return (
        f"<b>{BOT_NAME}</b>\n\n"
        "Ce bot est pensé comme une interface commerciale premium :\n"
        "• recherche prospects LinkedIn\n"
        "• recherche personne ciblée\n"
        "• recherche entreprise / dirigeant\n"
        "• export Excel immédiat\n"
        "• pagination et détail résultat par résultat\n\n"
        "<b>Format filtres</b>\n"
        "Envoie les filtres sous cette forme :\n"
        "<code>ville=Paris,pays=France,entreprise=Google,poste=Sales</code>\n\n"
        "Si tu n’as aucun filtre, réponds simplement <code>aucun</code>."
    )


def welcome_text() -> str:
    return (
        f"<b>{BOT_NAME}</b>\n"
        "Prospection intelligente • recherche ciblée • export premium\n\n"
        "Choisis un module pour démarrer."
    )


def search_summary_text(mode: str, query: str, filters: dict[str, str], count: int) -> str:
    filters_txt = ", ".join(f"{k}={v}" for k, v in filters.items()) if filters else "aucun"
    return (
        f"<b>Recherche terminée</b>\n"
        f"• Module : <b>{esc(mode_label(mode))}</b>\n"
        f"• Requête : <code>{esc(query)}</code>\n"
        f"• Filtres : <code>{esc(filters_txt)}</code>\n"
        f"• Résultats : <b>{count}</b>"
    )


def format_result_card(index: int, row: dict[str, Any], mode: str) -> str:
    title = row.get("Nom") or row.get("Entreprise") or row.get("Dirigeant") or "Résultat"
    lines = [f"<b>{index}. {esc(title)}</b>"]

    preferred_keys = {
        "prospect": ["Poste", "Entreprise", "Ville", "Pays", "LinkedIn", "Snippet"],
        "person": ["Nom", "Poste", "Entreprise", "Ville", "Pays", "LinkedIn", "Snippet"],
        "company": ["Entreprise", "Dirigeant", "SIREN", "Ville", "Source", "SourceLink", "Snippet"],
    }.get(mode, list(row.keys()))

    seen = set()
    for key in preferred_keys:
        value = row.get(key)
        if value:
            seen.add(key)
            lines.append(f"• <b>{esc(key)}</b> : {esc(value)}")

    for key, value in row.items():
        if key not in seen and value not in (None, ""):
            lines.append(f"• <b>{esc(key)}</b> : {esc(value)}")

    return "\n".join(lines)


def paginated_results_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    results = context.user_data.get("results") or []
    mode = context.user_data.get("mode", "prospect")
    page = int(context.user_data.get("page", 0))
    total_pages = max(1, (len(results) + SESSION_PAGE_SIZE - 1) // SESSION_PAGE_SIZE)
    start = page * SESSION_PAGE_SIZE
    end = min(len(results), start + SESSION_PAGE_SIZE)
    cards = [format_result_card(idx + 1, row, mode) for idx, row in enumerate(results[start:end], start=start)]

    summary = context.user_data.get("last_summary", "")
    header = summary + "\n\n" if summary else ""
    header += f"<b>Page {page + 1}/{total_pages}</b>\n"
    return header + "\n\n".join(cards)


def current_page_count(context: ContextTypes.DEFAULT_TYPE) -> int:
    results = context.user_data.get("results") or []
    page = int(context.user_data.get("page", 0))
    start = page * SESSION_PAGE_SIZE
    end = min(len(results), start + SESSION_PAGE_SIZE)
    return max(0, end - start)


async def safe_edit_or_send(
    *,
    target_message: Message,
    text: str,
    reply_markup: InlineKeyboardMarkup | None,
    parse_mode: str = ParseMode.HTML,
    disable_web_page_preview: bool = True,
) -> None:
    try:
        await target_message.edit_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )
    except BadRequest:
        await target_message.reply_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    clear_search_session(context)
    if update.message:
        await update.message.reply_text(welcome_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(help_text(), parse_mode=ParseMode.HTML, reply_markup=back_to_menu_keyboard())


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    clear_search_session(context)
    if update.message:
        await update.message.reply_text("Action annulée. Tu peux repartir sur une nouvelle recherche.", reply_markup=main_keyboard())


async def health_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            f"<b>{BOT_NAME}</b> est opérationnel.\nAPI locale : <code>{esc(settings.public_api_base_url)}</code>",
            parse_mode=ParseMode.HTML,
        )


async def last_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        if not context.user_data.get("results"):
            await update.message.reply_text("Aucune recherche récente disponible.", reply_markup=main_keyboard())
            return
        await update.message.reply_text(
            paginated_results_text(context),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=results_keyboard(
                int(context.user_data.get("page", 0)),
                max(1, (len(context.user_data.get("results") or []) + SESSION_PAGE_SIZE - 1) // SESSION_PAGE_SIZE),
                int(context.user_data.get("page", 0)) * SESSION_PAGE_SIZE,
                current_page_count(context),
            ),
        )


# ---------------------------------------------------------------------------
# Interactive flow
# ---------------------------------------------------------------------------
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()

    data = query.data or ""

    if data == "menu:home":
        clear_search_session(context)
        await safe_edit_or_send(target_message=query.message, text=welcome_text(), reply_markup=main_keyboard())
        return

    if data == "menu:help":
        await safe_edit_or_send(target_message=query.message, text=help_text(), reply_markup=back_to_menu_keyboard())
        return

    if data == "menu:last":
        if not context.user_data.get("results"):
            await query.message.reply_text("Aucune recherche récente disponible.", reply_markup=main_keyboard())
            return
        await render_results(query.message, context)
        return

    if data.startswith("mode:"):
        mode = data.split(":", 1)[1]
        clear_search_session(context)
        context.user_data["mode"] = mode
        context.user_data["flow"] = "awaiting_query"
        await query.message.reply_text(
            f"<b>{esc(mode_label(mode))}</b>\n\nEnvoie maintenant ta recherche principale.\n\n{mode_examples(mode)}",
            parse_mode=ParseMode.HTML,
            reply_markup=back_to_menu_keyboard(),
        )
        return

    if data.startswith("max:"):
        context.user_data["max_results"] = int(data.split(":", 1)[1])
        context.user_data["flow"] = "awaiting_export"
        await query.message.reply_text(
            f"Nombre maximum sélectionné : <b>{context.user_data['max_results']}</b>\n\nSouhaites-tu aussi générer l’Excel ?",
            parse_mode=ParseMode.HTML,
            reply_markup=export_choice_keyboard(),
        )
        return

    if data == "export:yes":
        context.user_data["export"] = True
        await launch_search(query.message, context)
        return

    if data == "export:no":
        context.user_data["export"] = False
        await launch_search(query.message, context)
        return

    if data == "page:prev":
        context.user_data["page"] = max(0, int(context.user_data.get("page", 0)) - 1)
        await render_results(query.message, context)
        return

    if data == "page:next":
        results = context.user_data.get("results") or []
        total_pages = max(1, (len(results) + SESSION_PAGE_SIZE - 1) // SESSION_PAGE_SIZE)
        context.user_data["page"] = min(total_pages - 1, int(context.user_data.get("page", 0)) + 1)
        await render_results(query.message, context)
        return

    if data.startswith("detail:"):
        index = int(data.split(":", 1)[1])
        results = context.user_data.get("results") or []
        if index < 0 or index >= len(results):
            await query.message.reply_text("Ce résultat n’est plus disponible. Relance une recherche.")
            return
        context.user_data["details_index"] = index
        row = results[index]
        mode = context.user_data.get("mode", "prospect")
        await safe_edit_or_send(
            target_message=query.message,
            text=format_result_card(index + 1, row, mode),
            reply_markup=detail_keyboard(),
        )
        return

    if data == "action:back_results":
        await render_results(query.message, context)
        return

    if data == "action:export":
        await export_current_results(query.message, context)
        return


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    text = (update.message.text or "").strip()
    flow = context.user_data.get("flow")

    if flow == "awaiting_query":
        context.user_data["query"] = text
        context.user_data["flow"] = "awaiting_filters"
        mode = context.user_data.get("mode", "prospect")
        hint = (
            "Envoie maintenant tes filtres au format :\n"
            "<code>ville=Paris,pays=France,entreprise=Google,poste=Sales</code>\n\n"
            "Réponds <code>aucun</code> si tu veux lancer sans filtre."
        )
        if mode == "company":
            hint = (
                "Envoie maintenant tes filtres.\n"
                "Pour la recherche entreprise, tu peux utiliser surtout :\n"
                "<code>ville=Paris</code>\n\n"
                "Ou réponds <code>aucun</code>."
            )
        await update.message.reply_text(hint, parse_mode=ParseMode.HTML, reply_markup=back_to_menu_keyboard())
        return

    if flow == "awaiting_filters":
        context.user_data["filters"] = parse_filters(text)
        context.user_data["flow"] = "awaiting_max_results"
        await update.message.reply_text(
            "Combien de résultats maximum veux-tu récupérer ?",
            reply_markup=max_results_keyboard(),
        )
        return

    await update.message.reply_text(
        "Utilise /start pour ouvrir le menu, ou /cancel pour annuler la session en cours.",
        reply_markup=main_keyboard(),
    )


# ---------------------------------------------------------------------------
# Search / render / export
# ---------------------------------------------------------------------------
async def launch_search(message: Message, context: ContextTypes.DEFAULT_TYPE) -> None:
    mode = context.user_data.get("mode")
    query = (context.user_data.get("query") or "").strip()
    filters = context.user_data.get("filters") or {}
    max_results = int(context.user_data.get("max_results") or MAX_RESULTS_DEFAULT)
    export_requested = bool(context.user_data.get("export"))

    if not mode or not query:
        await message.reply_text("Recherche incomplète. Repars avec /start.", reply_markup=main_keyboard())
        return

    context.user_data["flow"] = "searching"
    await message.reply_text("🔎 Recherche en cours…")

    try:
        if mode == "prospect":
            results = await asyncio.to_thread(search_prospects, query, filters, max_results)
        elif mode == "person":
            results = await asyncio.to_thread(search_person, query, filters, max_results)
        else:
            results = await asyncio.to_thread(search_company_person, query, filters.get("ville", ""))
            results = results[:max_results]
    except Exception as exc:
        logger.exception("Search failed")
        await message.reply_text(
            f"Une erreur est survenue pendant la recherche : <code>{esc(exc)}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(),
        )
        return

    if not results:
        context.user_data["flow"] = "idle"
        await message.reply_text(
            "Aucun résultat trouvé. Essaie avec une requête plus large ou moins de filtres.",
            reply_markup=main_keyboard(),
        )
        return

    context.user_data["results"] = results
    context.user_data["page"] = 0
    context.user_data["flow"] = "results_ready"
    context.user_data["last_summary"] = search_summary_text(mode, query, filters, len(results))

    await render_results(message, context)
    if export_requested:
        await export_current_results(message, context)


async def render_results(message: Message, context: ContextTypes.DEFAULT_TYPE) -> None:
    results = context.user_data.get("results") or []
    if not results:
        await message.reply_text("Aucun résultat à afficher.", reply_markup=main_keyboard())
        return

    page = int(context.user_data.get("page", 0))
    total_pages = max(1, (len(results) + SESSION_PAGE_SIZE - 1) // SESSION_PAGE_SIZE)
    start = page * SESSION_PAGE_SIZE

    await safe_edit_or_send(
        target_message=message,
        text=paginated_results_text(context),
        reply_markup=results_keyboard(page, total_pages, start, current_page_count(context)),
    )


async def export_current_results(message: Message, context: ContextTypes.DEFAULT_TYPE) -> None:
    results = context.user_data.get("results") or []
    query = context.user_data.get("query") or "export"
    if not results:
        await message.reply_text("Aucun résultat à exporter.")
        return

    await message.reply_text("📊 Génération de l’export Excel…")
    try:
        path: Path = await asyncio.to_thread(export_excel, results, query)
    except Exception as exc:
        logger.exception("Excel export failed")
        await message.reply_text(f"Impossible de générer l’export : <code>{esc(exc)}</code>", parse_mode=ParseMode.HTML)
        return

    with open(path, "rb") as file_obj:
        await message.reply_document(
            document=file_obj,
            filename=path.name,
            caption=f"Export prêt : {path.name}",
        )


# ---------------------------------------------------------------------------
# Error handler / runner
# ---------------------------------------------------------------------------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled bot error", exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="⚠️ Une erreur inattendue est survenue. Utilise /start pour relancer proprement.",
            )
    except Exception:
        logger.exception("Failed to send fallback error message")


def run_bot() -> None:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN manquant dans le .env")

    application = ApplicationBuilder().token(settings.telegram_bot_token).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(CommandHandler("health", health_command))
    application.add_handler(CommandHandler("last", last_command))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_error_handler(error_handler)
    application.run_polling(drop_pending_updates=True)
