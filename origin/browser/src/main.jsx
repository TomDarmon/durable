import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  ChevronRight,
  FileText,
  Folder,
  GitBranch,
  RefreshCw,
  Server,
} from "lucide-react";
import "./styles.css";

const api = async (path) => {
  const response = await fetch(path);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `HTTP ${response.status}`);
  }
  return response.json();
};

function App() {
  const [repositories, setRepositories] = useState([]);
  const [selectedRepo, setSelectedRepo] = useState(null);
  const [refs, setRefs] = useState([]);
  const [selectedRef, setSelectedRef] = useState("");
  const [path, setPath] = useState("");
  const [entries, setEntries] = useState([]);
  const [blob, setBlob] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const selectedRepoKey = selectedRepo
    ? `${selectedRepo.tenant}/${selectedRepo.name}`
    : "";

  const branchRefs = useMemo(
    () => refs.filter((ref) => ref.name.startsWith("refs/heads/")),
    [refs],
  );

  const loadRepositories = async () => {
    setLoading(true);
    setError("");
    try {
      const data = await api("/api/repos");
      setRepositories(data.repositories ?? []);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const openRepository = async (repo) => {
    setSelectedRepo(repo);
    setRefs([]);
    setSelectedRef("");
    setPath("");
    setEntries([]);
    setBlob(null);
    setLoading(true);
    setError("");
    try {
      const data = await api(`/api/repos/${repo.tenant}/${repo.name}/refs`);
      const nextRefs = data.refs ?? [];
      setRefs(nextRefs);
      const defaultRef =
        nextRefs.find((ref) => ref.name === "refs/heads/main") ??
        nextRefs.find((ref) => ref.name.startsWith("refs/heads/")) ??
        nextRefs[0];
      if (defaultRef) {
        setSelectedRef(defaultRef.name);
        await openTree(repo, defaultRef.name, "");
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const openTree = async (repo, reference, nextPath) => {
    setLoading(true);
    setError("");
    try {
      const query = new URLSearchParams({ ref: reference });
      if (nextPath) query.set("path", nextPath);
      const data = await api(
        `/api/repos/${repo.tenant}/${repo.name}/tree?${query.toString()}`,
      );
      setPath(data.path ?? "");
      setEntries(data.entries ?? []);
      setBlob(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const openBlob = async (entry) => {
    if (!selectedRepo || !selectedRef) return;
    setLoading(true);
    setError("");
    try {
      const query = new URLSearchParams({
        ref: selectedRef,
        path: entry.path,
      });
      const data = await api(
        `/api/repos/${selectedRepo.tenant}/${selectedRepo.name}/blob?${query.toString()}`,
      );
      setBlob(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadRepositories();
  }, []);

  const breadcrumbs = path ? path.split("/") : [];

  return (
    <main className="shell">
      <aside className="sidebar">
        <div className="brand">
          <Server size={20} />
          <span>Origin</span>
          <button
            className="icon-button"
            type="button"
            aria-label="Refresh repositories"
            title="Refresh repositories"
            onClick={loadRepositories}
          >
            <RefreshCw size={16} />
          </button>
        </div>
        <div className="repo-list">
          {repositories.map((repo) => {
            const key = `${repo.tenant}/${repo.name}`;
            return (
              <button
                className={`repo-item ${key === selectedRepoKey ? "active" : ""}`}
                key={key}
                type="button"
                onClick={() => openRepository(repo)}
              >
                <span>{repo.name}</span>
                <small>{repo.tenant}</small>
              </button>
            );
          })}
          {!repositories.length && <p className="muted">No repositories yet.</p>}
        </div>
      </aside>

      <section className="workspace">
        <header className="repo-header">
          <div>
            <p className="eyebrow">Repository</p>
            <h1>{selectedRepo ? selectedRepo.name : "Select a repository"}</h1>
          </div>
          {selectedRepo && (
            <label className="ref-picker">
              <GitBranch size={16} />
              <select
                value={selectedRef}
                onChange={(event) => {
                  const nextRef = event.target.value;
                  setSelectedRef(nextRef);
                  openTree(selectedRepo, nextRef, "");
                }}
              >
                {branchRefs.map((ref) => (
                  <option key={ref.name} value={ref.name}>
                    {ref.name.replace("refs/heads/", "")}
                  </option>
                ))}
                {refs
                  .filter((ref) => !ref.name.startsWith("refs/heads/"))
                  .map((ref) => (
                    <option key={ref.name} value={ref.name}>
                      {ref.name}
                    </option>
                  ))}
              </select>
            </label>
          )}
        </header>

        {error && <div className="error">{error}</div>}
        {loading && <div className="loading">Loading</div>}

        {selectedRepo && selectedRef && (
          <>
            <nav className="breadcrumbs" aria-label="Path">
              <button
                type="button"
                onClick={() => openTree(selectedRepo, selectedRef, "")}
              >
                {selectedRepo.name}
              </button>
              {breadcrumbs.map((part, index) => {
                const nextPath = breadcrumbs.slice(0, index + 1).join("/");
                return (
                  <React.Fragment key={nextPath}>
                    <ChevronRight size={14} />
                    <button
                      type="button"
                      onClick={() => openTree(selectedRepo, selectedRef, nextPath)}
                    >
                      {part}
                    </button>
                  </React.Fragment>
                );
              })}
            </nav>

            <div className="browser-grid">
              <section className="file-panel">
                {entries.map((entry) => (
                  <button
                    className="file-row"
                    key={entry.path}
                    type="button"
                    onClick={() =>
                      entry.kind === "tree"
                        ? openTree(selectedRepo, selectedRef, entry.path)
                        : openBlob(entry)
                    }
                  >
                    {entry.kind === "tree" ? (
                      <Folder size={17} />
                    ) : (
                      <FileText size={17} />
                    )}
                    <span>{entry.name}</span>
                    <small>{entry.size == null ? "" : `${entry.size} B`}</small>
                  </button>
                ))}
                {!entries.length && <p className="muted">Empty tree.</p>}
              </section>

              <section className="blob-panel">
                {blob ? (
                  <>
                    <div className="blob-title">
                      <FileText size={17} />
                      <span>{blob.path}</span>
                    </div>
                    <pre>{blob.content}</pre>
                  </>
                ) : (
                  <p className="muted">Select a file.</p>
                )}
              </section>
            </div>
          </>
        )}
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")).render(<App />);
