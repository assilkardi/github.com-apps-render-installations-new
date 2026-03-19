import { useEffect, useState } from "react";
import { getAdminStats } from "./api";
import "./App.css";

export default function AdminDashboard({ adminToken, onBack, onLogout }) {
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState(null);
  const [error, setError] = useState("");

  async function loadStats() {
    setLoading(true);
    setError("");

    try {
      const data = await getAdminStats(adminToken);
      setStats(data);
    } catch (err) {
      setError(err.message || "Accès admin refusé.");
      setStats(null);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadStats();
  }, []);

  return (
    <div className="app-shell">
      <div className="app-container">
        <header className="hero-card">
          <div className="hero-grid">
            <div>
              <div className="badge">🔐 Zone sécurisée</div>
              <h1 className="hero-title">Dashboard Admin</h1>
              <p className="hero-text">
                Suivi interne du moteur LeadGen, état des accès, cache, providers
                et santé globale de l’API.
              </p>

              <div className="stats-grid">
                <div className="stat-card">
                  <div className="stat-label">API</div>
                  <div className="stat-value">
                    {stats?.ok ? "En ligne" : "Inconnue"}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Cache</div>
                  <div className="stat-value">
                    {stats?.cache_entries ?? 0} entrée(s)
                  </div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Cooldown SerpAPI</div>
                  <div className="stat-value">
                    {stats?.serpapi_cooldown_seconds ?? 0}s
                  </div>
                </div>
              </div>
            </div>

            <div className="side-panel">
              <div className="side-head">
                <div>
                  <div className="side-label">Session</div>
                  <div className="side-user">Admin • LeadGen</div>
                </div>
                <div className="online-pill">Sécurisé</div>
              </div>

              <div className="button-stack">
                <button className="btn btn-light" onClick={loadStats}>
                  {loading ? "Actualisation..." : "Actualiser"}
                </button>
                <button className="btn btn-outline" onClick={onBack}>
                  Retour recherche
                </button>
                <button className="btn btn-outline" onClick={onLogout}>
                  Déconnexion
                </button>
              </div>

              {error && <div className="note-card">{error}</div>}
            </div>
          </div>
        </header>

        <section className="content-grid">
          <div className="panel-card">
            <div className="panel-head">
              <div>
                <h3 className="panel-title">Accès utilisateurs</h3>
                <p className="panel-subtitle">
                  État des utilisateurs et demandes d’accès.
                </p>
              </div>
            </div>

            <div className="results-list">
              <div className="result-card">
                <div className="result-name">Utilisateurs approuvés</div>
                <div className="result-role">
                  {stats?.approved_users ?? 0}
                </div>
              </div>

              <div className="result-card">
                <div className="result-name">Demandes en attente</div>
                <div className="result-role">
                  {stats?.pending_users ?? 0}
                </div>
              </div>

              <div className="result-card">
                <div className="result-name">Blacklist</div>
                <div className="result-role">{stats?.blacklist ?? 0}</div>
              </div>
            </div>
          </div>

          <div className="panel-card">
            <div className="panel-head">
              <div>
                <h3 className="panel-title">Providers</h3>
                <p className="panel-subtitle">
                  Suivi de disponibilité et statistiques.
                </p>
              </div>
            </div>

            <div className="results-list">
              {stats?.provider_stats ? (
                Object.entries(stats.provider_stats).map(([provider, providerData]) => (
                  <div key={provider} className="result-card">
                    <div className="result-top">
                      <div>
                        <div className="result-name">{provider}</div>
                        <div className="result-role">
                          Success: {providerData?.success ?? 0}
                        </div>
                        <div className="result-meta">
                          Failures: {providerData?.failures ?? 0}
                        </div>
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <div className="result-card empty-card">
                  Aucune statistique provider disponible.
                </div>
              )}
            </div>
          </div>
        </section>

        <section className="content-grid">
          <div className="panel-card">
            <div className="panel-head">
              <div>
                <h3 className="panel-title">Santé système</h3>
                <p className="panel-subtitle">
                  Informations utiles côté moteur.
                </p>
              </div>
            </div>

            <div className="results-list">
              <div className="result-card">
                <div className="result-name">Service</div>
                <div className="result-role">{stats?.service || "LeadGen API"}</div>
              </div>

              <div className="result-card">
                <div className="result-name">Origines CORS</div>
                <div className="result-role">
                  {Array.isArray(stats?.cors)
                    ? stats.cors.join(", ")
                    : "Non disponible"}
                </div>
              </div>

              <div className="result-card">
                <div className="result-name">Serper cooldown</div>
                <div className="result-role">
                  {stats?.serper_cooldown_seconds ?? 0}s
                </div>
              </div>
            </div>
          </div>

          <div className="panel-card">
            <div className="panel-head">
              <div>
                <h3 className="panel-title">Statut</h3>
                <p className="panel-subtitle">
                  Vue synthétique du back-office.
                </p>
              </div>
            </div>

            <div className="results-list">
              <div className="result-card">
                <div className="result-name">API</div>
                <div className="result-role">
                  {stats?.ok ? "Fonctionnelle" : "Indisponible"}
                </div>
              </div>

              <div className="result-card">
                <div className="result-name">Cache moteur</div>
                <div className="result-role">
                  {stats?.cache_entries ?? 0} élément(s)
                </div>
              </div>

              <div className="result-card">
                <div className="result-name">Mode admin</div>
                <div className="result-role">Authentifié</div>
              </div>
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}