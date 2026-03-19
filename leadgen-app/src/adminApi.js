const API_BASE_URL = import.meta.env.DEV
  ? "http://127.0.0.1:8000"
  : "https://leadgen-api-8d37.onrender.com";

export async function getAdminHealth() {
  const response = await fetch(`${API_BASE_URL}/health`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
    },
  });

  const data = await response.json();

  if (!response.ok) {
    throw new Error(data?.message || "Erreur API admin");
  }

  return data;
}