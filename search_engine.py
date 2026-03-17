from serpapi import GoogleSearch
from config import SERPAPI_KEY


def clean_title(title):

    if not title:
        return "", "", ""

    title = title.replace("| LinkedIn", "").strip()

    parts = title.split(" - ")

    nom = parts[0] if len(parts) > 0 else ""
    poste = parts[1] if len(parts) > 1 else ""
    entreprise = parts[2] if len(parts) > 2 else ""

    return nom, poste, entreprise


def search_linkedin(keyword, filters=None, limit=100):

    if filters is None:
        filters = {}

    query = f"site:linkedin.com/in {keyword}"

    for key in filters:
        query += f" {filters[key]}"

    params = {
        "engine": "google",
        "q": query,
        "api_key": SERPAPI_KEY,
        "num": limit
    }

    search = GoogleSearch(params)
    results = search.get_dict()

    prospects = []
    seen = set()

    if "organic_results" in results:

        for r in results["organic_results"]:

            link = r.get("link", "")
            title = r.get("title", "")

            if "linkedin.com/in" not in link:
                continue

            if link in seen:
                continue

            seen.add(link)

            nom, poste, entreprise = clean_title(title)

            prospects.append({
                "Nom": nom,
                "Poste": poste,
                "Entreprise": entreprise,
                "LinkedIn": link
            })

    return prospects