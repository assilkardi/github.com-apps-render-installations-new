const API_BASE_URL = import.meta.env.DEV
  ? "http://127.0.0.1:8000"
  : "https://leadgen-api-8d37.onrender.com";

export const APP_API_BASE_URL = API_BASE_URL;

export async function runSearch(payload) {
  const response = await fetch(`${API_BASE_URL}/search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();

  if (!response.ok) {
    throw new Error(data?.message || "Erreur API");
  }

  return data;
}

export async function getAdminStats(adminToken) {
  const response = await fetch(`${API_BASE_URL}/admin/stats`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      "X-Admin-Token": adminToken,
    },
  });

  const data = await response.json();

  if (!response.ok) {
    throw new Error(data?.detail || "Accès admin refusé.");
  }

  return data;
}