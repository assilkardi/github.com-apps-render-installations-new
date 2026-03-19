import { useMemo, useState } from "react";
import "./App.css";
import { APP_API_BASE_URL, runSearch } from "./api";
import AdminDashboard from "./AdminDashboard";

const INITIAL_LIMIT = 10;
const LOAD_MORE_STEP = 10;

export default function App() {
  const [mode, setMode] = useState("prospect");

  const [query, setQuery] = useState("");
  const [ville, setVille] = useState("");
  const [pays, setPays] = useState("");
  const [entreprise, setEntreprise] = useState("");
  const [poste, setPoste] = useState("");

  const [fuzzyEnabled, setFuzzyEnabled] = useState(true);
  const [exportExcelRequested, setExportExcelRequested] = useState(false);

  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);

  const [results, setResults] = useState([]);
  const [message, setMessage] = useState("");
  const [count, setCount] = useState(0);
  const [excelFile, setExcelFile] = useState("");
  const [requestedLimit, setRequestedLimit] = useState(INITIAL_LIMIT);
  const [hasSearched, setHasSearched] = useState(false);

  const [showAdminLogin, setShowAdminLogin] = useState(false);
  const [adminAuthenticated, setAdminAuthenticated] = useState(false);
  const [adminToken, setAdminToken] = useState("");
  const [adminDraftToken, setAdminDraftToken] = useState("");
  const [adminError, setAdminError] = useState("");

  const modules = [
    {
      id: "prospect",
      title: "Prospection LinkedIn",
      subtitle: "Trouver des profils qualifiés avec nom, poste et lien LinkedIn.",
      icon: "🚀",
    },
    {
      id: "company",
      title: "Entreprises & dirigeants",
      subtitle: "Identifier le dirigeant, l’entreprise, le SIREN et la source.",
      icon: "🏢",
    },
  ];

  const filters = useMemo(() => {
    const f = {};

    if (ville.trim()) f.ville = ville.trim();

    if (mode === "prospect") {
      if (pays.trim()) f.pays = pays.trim();
      if (entreprise.trim()) f.entreprise = entreprise.trim();
      if (poste.trim()) f.poste = poste.trim();
    }

    return f;
  }, [ville, pays, entreprise, poste, mode]);

  const modeConfig =
    mode === "prospect"
      ? {
          formTitle: "Nouvelle recherche LinkedIn",
          formSubtitle:
            "Saisis un poste, un mot-clé ou une fonction pour obtenir des profils qualifiés.",
          mainLabel: "Poste / mot-clé / fonction",
          mainPlaceholder:
            "Ex. business developer, responsable RH, développeur full stack",
          buttonLabel: "Lancer la prospection",
          resultTitle: "Profils trouvés",
          emptyText: "Aucun profil affiché pour le moment.",
          helperText:
            "Résultats avec nom, poste, entreprise et lien direct vers LinkedIn.",
        }
      : {
          formTitle: "Nouvelle recherche entreprise",
          formSubtitle:
            "Saisis un nom, un patronyme ou une société pour retrouver dirigeant, entreprise et SIREN.",
          mainLabel: "Nom / entreprise / dirigeant",
          mainPlaceholder: "Ex. Dupont, Jerome BENHAMOU, Martin & Co",
          buttonLabel: "Rechercher l’entreprise",
          resultTitle: "Entreprises trouvées",
          emptyText: "Aucun résultat entreprise affiché pour le moment.",
          helperText:
            "Résultats avec nom du dirigeant, entreprise, SIREN et lien source.",
        };

  function resetSharedStates() {
    setResults([]);
    setMessage("");
    setCount(0);
    setExcelFile("");
    setRequestedLimit(INITIAL_LIMIT);
    setHasSearched(false);
    setLoading(false);
    setLoadingMore(false);
  }

  function switchMode(nextMode) {
    setMode(nextMode);
    setQuery("");
    setVille("");
    setPays("");
    setEntreprise("");
    setPoste("");
    setFuzzyEnabled(true);
    setExportExcelRequested(false);
    resetSharedStates();
  }

  function handleReset() {
    setMode("prospect");
    setQuery("");
    setVille("");
    setPays("");
    setEntreprise("");
    setPoste("");
    setFuzzyEnabled(true);
    setExportExcelRequested(false);
    resetSharedStates();
  }

  async function executeSearch(limit) {
    if (!query.trim()) {
      setMessage(
        mode === "prospect"
          ? "Merci de saisir un poste, un mot-clé ou une fonction."
          : "Merci de saisir un nom ou une entreprise."
      );
      return;
    }

    setLoading(true);
    setMessage("");
    setResults([]);
    setCount(0);
    setExcelFile("");

    try {
      const data = await runSearch({
        mode,
        query: query.trim(),
        filters,
        fuzzy_enabled: mode === "prospect" ? fuzzyEnabled : false,
        export_excel_requested: exportExcelRequested,
        max_results: limit,
      });

      if (!data.success) {
        setMessage(data.message || "Une erreur est survenue.");
        setResults([]);
        setCount(0);
        setExcelFile("");
        setHasSearched(true);
        return;
      }

      const incomingResults = Array.isArray(data.results) ? data.results : [];

      setResults(incomingResults);
      setCount(data.count || incomingResults.length);
      setExcelFile(data.excel_file || "");
      setMessage(data.message || "Recherche terminée.");
      setRequestedLimit(limit);
      setHasSearched(true);
    } catch (error) {
      setMessage("Impossible de contacter l’API.");
      setResults([]);
      setCount(0);
      setExcelFile("");
      setHasSearched(true);
    } finally {
      setLoading(false);
    }
  }

  async function handleSearch() {
    await executeSearch(INITIAL_LIMIT);
  }

  async function handleLoadMore() {
    const nextLimit = requestedLimit + LOAD_MORE_STEP;
    setLoadingMore(true);
    await executeSearch(nextLimit);
    setLoadingMore(false);
  }

  function openAdminLogin() {
    setShowAdminLogin(true);
    setAdminError("");
  }

  function closeAdminLogin() {
    setShowAdminLogin(false);
    setAdminDraftToken("");
    setAdminError("");
  }

  function handleAdminLogin() {
    if (!adminDraftToken.trim()) {
      setAdminError("Merci de saisir le token admin.");
      return;
    }

    setAdminToken(adminDraftToken.trim());
    setAdminAuthenticated(true);
    setShowAdminLogin(false);
    setAdminError("");
  }

  function handleAdminLogout() {
    setAdminAuthenticated(false);
    setAdminToken("");
    setAdminDraftToken("");
    setAdminError("");
  }

  const canLoadMore = results.length >= requestedLimit && results.length > 0;

  if (adminAuthenticated) {
    return (
      <AdminDashboard
        adminToken={adminToken}
        onBack={() => setAdminAuthenticated(false)}
        onLogout={handleAdminLogout}
      />
    );
  }

  return (
    <div className="app-shell">
      <div className="app-container">
        <header className="hero-card">
          <div className="hero-grid">
            <div>
              <div className="badge">✨ Telegram Mini App premium</div>
              <h1 className="hero-title">LeadGen Premium</h1>
              <p className="hero-text">
                Une interface haut de gamme pour lancer des recherches LinkedIn
                et entreprises avec une expérience plus claire, plus rapide et
                plus professionnelle.
              </p>

              <div className="stats-grid">
                <div className="stat-card">
                  <div className="stat-label">Recherche</div>
                  <div className="stat-value">LinkedIn</div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Données</div>
                  <div className="stat-value">Entreprise + dirigeants</div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Export</div>
                  <div className="stat-value">Excel structuré</div>
                </div>
              </div>
            </div>

            <div className="side-panel">
              <div className="side-head">
                <div>
                  <div className="side-label">Espace</div>
                  <div className="side-user">LeadGen • Dashboard</div>
                </div>
                <div className="online-pill">Actif</div>
              </div>

              <div className="button-stack">
                <button className="btn btn-light" onClick={handleSearch}>
                  {loading ? "Recherche..." : modeConfig.buttonLabel}
                </button>
                <button className="btn btn-outline" onClick={handleReset}>
                  Réinitialiser
                </button>
                <button className="btn btn-outline" onClick={openAdminLogin}>
                  Dashboard admin
                </button>
              </div>

              <div className="note-card">
                <div className="stat-label">Positionnement</div>
                <div className="note-text">
                  Outil de prospection premium pensé comme un mini CRM visuel
                  directement dans Telegram.
                </div>
              </div>
            </div>
          </div>
        </header>

        <section className="modules-grid">
          {modules.map((module) => (
            <div
              key={module.id}
              className={`module-card ${
                mode === module.id ? "module-card-active" : ""
              }`}
              onClick={() => switchMode(module.id)}
            >
              <div className="module-icon">{module.icon}</div>
              <h2 className="module-title">{module.title}</h2>
              <p className="module-subtitle">{module.subtitle}</p>
              <button className="btn btn-outline small-btn">
                {mode === module.id ? "Sélectionné" : "Choisir"}
              </button>
            </div>
          ))}
        </section>

        <section className="content-grid">
          <div className="panel-card">
            <div className="panel-head">
              <div>
                <h3 className="panel-title">{modeConfig.formTitle}</h3>
                <p className="panel-subtitle">{modeConfig.formSubtitle}</p>
              </div>
              <div className="panel-chip">{mode}</div>
            </div>

            <div className="helper-box">{modeConfig.helperText}</div>

            <div className="form-grid">
              <div>
                <label className="input-label">{modeConfig.mainLabel}</label>
                <input
                  className="text-input"
                  placeholder={modeConfig.mainPlaceholder}
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                />
              </div>

              {mode === "prospect" ? (
                <>
                  <div className="two-cols">
                    <div>
                      <label className="input-label">Ville</label>
                      <input
                        className="text-input"
                        placeholder="Paris"
                        value={ville}
                        onChange={(e) => setVille(e.target.value)}
                      />
                    </div>
                    <div>
                      <label className="input-label">Pays</label>
                      <input
                        className="text-input"
                        placeholder="France"
                        value={pays}
                        onChange={(e) => setPays(e.target.value)}
                      />
                    </div>
                  </div>

                  <div className="two-cols">
                    <div>
                      <label className="input-label">Entreprise</label>
                      <input
                        className="text-input"
                        placeholder="Airbus"
                        value={entreprise}
                        onChange={(e) => setEntreprise(e.target.value)}
                      />
                    </div>
                    <div>
                      <label className="input-label">Poste</label>
                      <input
                        className="text-input"
                        placeholder="Responsable RH"
                        value={poste}
                        onChange={(e) => setPoste(e.target.value)}
                      />
                    </div>
                  </div>

                  <div className="two-cols">
                    <label className="check-card">
                      <input
                        type="checkbox"
                        checked={fuzzyEnabled}
                        onChange={(e) => setFuzzyEnabled(e.target.checked)}
                      />
                      <span>Orthographe approchante</span>
                    </label>

                    <label className="check-card">
                      <input
                        type="checkbox"
                        checked={exportExcelRequested}
                        onChange={(e) =>
                          setExportExcelRequested(e.target.checked)
                        }
                      />
                      <span>Export Excel souhaité</span>
                    </label>
                  </div>
                </>
              ) : (
                <>
                  <div>
                    <label className="input-label">Ville</label>
                    <input
                      className="text-input"
                      placeholder="Paris"
                      value={ville}
                      onChange={(e) => setVille(e.target.value)}
                    />
                  </div>

                  <label className="check-card">
                    <input
                      type="checkbox"
                      checked={exportExcelRequested}
                      onChange={(e) =>
                        setExportExcelRequested(e.target.checked)
                      }
                    />
                    <span>Export Excel souhaité</span>
                  </label>
                </>
              )}

              <div className="button-row">
                <button className="btn btn-light" onClick={handleSearch}>
                  {loading ? "Recherche..." : modeConfig.buttonLabel}
                </button>
                <button className="btn btn-outline" onClick={handleReset}>
                  Réinitialiser
                </button>
              </div>

              {message && <div className="note-card">{message}</div>}
            </div>
          </div>

          <div className="panel-card">
            <div className="panel-head">
              <div>
                <h3 className="panel-title">{modeConfig.resultTitle}</h3>
                <p className="panel-subtitle">
                  Résultats réels renvoyés par ton moteur Python.
                </p>
              </div>
              <div className="info-pill">{count} résultat(s)</div>
            </div>

            <div className="results-list">
              {!loading && results.length === 0 && (
                <div className="result-card empty-card">
                  {modeConfig.emptyText}
                </div>
              )}

              {mode === "prospect" &&
                results.map((card, index) => (
                  <div key={index} className="result-card">
                    <div className="result-top">
                      <div>
                        <div className="result-name">
                          {card.Nom || "Nom inconnu"}
                        </div>
                        <div className="result-role">
                          {card.Poste || "Poste non renseigné"}
                        </div>
                        <div className="result-meta">
                          {card.Entreprise || "Entreprise non renseignée"}
                        </div>
                      </div>

                      <div className="result-tags">
                        {card.MatchScore && (
                          <span className="score-pill">
                            Score {card.MatchScore}
                          </span>
                        )}
                      </div>
                    </div>

                    <div className="button-row wrap">
                      {card.LinkedIn ? (
                        <a
                          className="btn btn-light small-btn"
                          href={card.LinkedIn}
                          target="_blank"
                          rel="noreferrer"
                        >
                          Voir le profil LinkedIn
                        </a>
                      ) : (
                        <span className="result-link-missing">
                          Lien LinkedIn non disponible
                        </span>
                      )}
                    </div>
                  </div>
                ))}

              {mode === "company" &&
                results.map((card, index) => (
                  <div key={index} className="result-card">
                    <div className="result-top">
                      <div>
                        <div className="result-name">
                          {card.Dirigeant || "Dirigeant non renseigné"}
                        </div>
                        <div className="result-role">
                          {card.Entreprise_INPI || "Entreprise non renseignée"}
                        </div>
                        <div className="result-meta">
                          SIREN : {card.SIREN || "Non renseigné"}
                        </div>
                      </div>

                      <div className="result-tags">
                        {card.SIREN && (
                          <span className="success-pill">
                            SIREN {card.SIREN}
                          </span>
                        )}
                      </div>
                    </div>

                    <div className="button-row wrap">
                      {card.Lien_source ? (
                        <a
                          className="btn btn-outline small-btn"
                          href={card.Lien_source}
                          target="_blank"
                          rel="noreferrer"
                        >
                          Voir la source
                        </a>
                      ) : (
                        <span className="result-link-missing">
                          Source non disponible
                        </span>
                      )}
                    </div>
                  </div>
                ))}
            </div>

            {hasSearched && canLoadMore && (
              <div className="load-more-row">
                <button
                  className="btn btn-outline"
                  onClick={handleLoadMore}
                  disabled={loadingMore || loading}
                >
                  {loadingMore
                    ? "Chargement..."
                    : `Charger ${LOAD_MORE_STEP} résultats de plus`}
                </button>
              </div>
            )}

            {excelFile && (
              <div className="load-more-row">
                <a
                  className="btn btn-gold"
                  href={`${APP_API_BASE_URL}/download/${excelFile}`}
                  target="_blank"
                  rel="noreferrer"
                >
                  Télécharger l’Excel
                </a>
              </div>
            )}
          </div>
        </section>

        {showAdminLogin && (
          <div
            style={{
              position: "fixed",
              inset: 0,
              background: "rgba(0,0,0,0.55)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: "20px",
              zIndex: 9999,
            }}
          >
            <div
              className="panel-card"
              style={{
                width: "100%",
                maxWidth: "460px",
              }}
            >
              <div className="panel-head">
                <div>
                  <h3 className="panel-title">Connexion admin</h3>
                  <p className="panel-subtitle">
                    Saisis le token admin configuré côté backend.
                  </p>
                </div>
              </div>

              <div className="form-grid">
                <div>
                  <label className="input-label">Token admin</label>
                  <input
                    className="text-input"
                    type="password"
                    placeholder="Entrer le token admin"
                    value={adminDraftToken}
                    onChange={(e) => setAdminDraftToken(e.target.value)}
                  />
                </div>

                {adminError && <div className="note-card">{adminError}</div>}

                <div className="button-row">
                  <button className="btn btn-light" onClick={handleAdminLogin}>
                    Ouvrir le dashboard
                  </button>
                  <button className="btn btn-outline" onClick={closeAdminLogin}>
                    Annuler
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}