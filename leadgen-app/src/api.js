const API_BASE_URL = import.meta.env.DEV
  ? "http://127.0.0.1:8000"
  : "https://TON-API-RENDER.onrender.com";

export const APP_API_BASE_URL = API_BASE_URL;

export async function runSearch(payload) {
  const response = await fetch(`${API_BASE_URL}/search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error("Erreur API");
  }

  return response.json();
} 