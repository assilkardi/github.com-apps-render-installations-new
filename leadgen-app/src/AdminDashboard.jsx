import { useEffect, useState } from "react";
import { getAdminHealth } from "./adminApi";

export default function AdminDashboard({ onBack }) {
  const [loading, setLoading] = useState(true);
  const [health, setHealth] = useState(null);
  const [error, setError] = useState("");

  async function loadHealth() {
    try {
      setLoading(true);
      setError("");
      const data = await getAdminHealth();
      setHealth(data);
    } catch (err) {
      setError("Impossible de charger le dashboard admin.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadHealth();
  }, []);

  const providerStats = health?.provider_stats || {};

  return (
    <section className="content-grid">
      <div className="panel-card">
        <div className="panel-head">
          <div>
            <h3 className="panel-title">Vue globale</h3>
            <p className="panel-subtitle">
              Santé générale de l’API et du moteur LeadGen.
            </p>
          </div>
          <div className="info-pill">{health?.ok ? "API OK" : "..."}</div>
        </div>

        <div className="button-row">
          <button className="btn btn-light" onClick={loadHealth}>
            {loading ? "Actualisation..." : "Actualiser"}
          </button>
          <button className="btn btn-outline" onClick={onBack}>
            Retour
          </button>
        </div>

        {error && <div className="note-card">{error}</div>}

        {!loading && health && (
          <div className="results-list">
            <div className="result-card">
              <div className="result-name">Service</div>
              <div className="result-meta">{health.service || "LeadGen API"}</div>
            </div>

            <div className="result-card">
              <div className="result-name">Entrées cache</div>
              <div className="result-meta">{health.cache_entries ?? 0}</div>
            </div>

            <div className="result-card">
              <div className="result-name">Utilisateurs approuvés</div>
              <div className="result-meta">{health.approved_users ?? 0}</div>
            </div>

            <div className="result-card">
              <div className="result-name">Demandes en attente</div>
              <div className="result-meta">{health.pending_users ?? 0}</div>
            </div>

            <div className="result-card">
              <div className="result-name">Blacklist</div>
              <div className="result-meta">{health.blacklist ?? 0}</div>
            </div>

            <div className="result-card">
              <div className="result-name">Cooldown SerpAPI</div>
              <div className="result-meta">
                {health.serpapi_cooldown_seconds ?? 0} sec
              </div>
            </div>

            <div className="result-card">
              <div className="result-name">CORS</div>
              <div className="result-meta">
                {Array.isArray(health.cors) ? health.cors.join(", ") : "Non défini"}
              </div>
            </div>
          </div>
        )}
      </div>

      <div className="panel-card">
        <div className="panel-head">
          <div>
            <h3 className="panel-title">Providers</h3>
            <p className="panel-subtitle">
              Statistiques remontées par ton backend Python.
            </p>
          </div>
        </div>

        <div className="results-list">
          {loading && <div className="result-card empty-card">Chargement...</div>}

          {!loading && Object.keys(providerStats).length === 0 && (
            <div className="result-card empty-card">
              Aucune statistique provider disponible.
            </div>
          )}

          {!loading &&
            Object.entries(providerStats).map(([providerName, stats]) => (
              <div key={providerName} className="result-card">
                <div className="result-top">
                  <div>
                    <div className="result-name">{providerName}</div>
                    <div className="result-role">
                      Succès : {stats?.success ?? 0}
                    </div>
                    <div className="result-meta">
                      Erreurs : {stats?.errors ?? 0}
                    </div>
                  </div>

                  <div className="result-tags">
                    <span className="success-pill">
                      Total {(stats?.success ?? 0) + (stats?.errors ?? 0)}
                    </span>
                  </div>
                </div>
              </div>
            ))}
        </div>
      </div>
    </section>
  );
}