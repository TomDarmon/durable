import React, { useEffect, useMemo, useState } from "react";
import {
  ChevronRight,
  FileText,
  Folder,
  GitBranch,
  RefreshCw,
  Server,
} from "lucide-react";
import type { GitRef, Repository } from "../server/originBackend";
import { trpc } from "../utils/trpc";

const emptyRepository: Repository = { tenant: "", name: "" };

function defaultRef(refs: GitRef[]): GitRef | null {
  return (
    refs.find((ref) => ref.name === "refs/heads/main") ??
    refs.find((ref) => ref.name.startsWith("refs/heads/")) ??
    refs[0] ??
    null
  );
}

export default function Home() {
  const [selectedRepo, setSelectedRepo] = useState<Repository | null>(null);
  const [selectedRef, setSelectedRef] = useState("");
  const [path, setPath] = useState("");
  const [blobPath, setBlobPath] = useState("");

  const repositoriesQuery = trpc.repositories.list.useQuery(undefined, {
    staleTime: 2_000,
  });
  const refsQuery = trpc.repositories.refs.useQuery(selectedRepo ?? emptyRepository, {
    enabled: Boolean(selectedRepo),
  });
  const treeQuery = trpc.repositories.tree.useQuery(
    selectedRepo && selectedRef
      ? {
          ...selectedRepo,
          reference: selectedRef,
          path,
        }
      : {
          ...emptyRepository,
          reference: "",
          path: "",
        },
    {
      enabled: Boolean(selectedRepo && selectedRef),
    },
  );
  const blobQuery = trpc.repositories.blob.useQuery(
    selectedRepo && selectedRef && blobPath
      ? {
          ...selectedRepo,
          reference: selectedRef,
          path: blobPath,
        }
      : {
          ...emptyRepository,
          reference: "",
          path: "",
        },
    {
      enabled: Boolean(selectedRepo && selectedRef && blobPath),
    },
  );

  const repositories = repositoriesQuery.data?.repositories ?? [];
  const refs = refsQuery.data?.refs ?? [];
  const entries = treeQuery.data?.entries ?? [];
  const blob = blobQuery.data ?? null;

  const selectedRepoKey = selectedRepo
    ? `${selectedRepo.tenant}/${selectedRepo.name}`
    : "";

  const branchRefs = useMemo(
    () => refs.filter((ref) => ref.name.startsWith("refs/heads/")),
    [refs],
  );

  useEffect(() => {
    if (!selectedRepo) return;
    const nextDefault = defaultRef(refs);
    if (!nextDefault) {
      setSelectedRef("");
      return;
    }
    if (!refs.some((ref) => ref.name === selectedRef)) {
      setSelectedRef(nextDefault.name);
      setPath("");
      setBlobPath("");
    }
  }, [refs, selectedRef, selectedRepo]);

  const openRepository = (repo: Repository) => {
    setSelectedRepo(repo);
    setSelectedRef("");
    setPath("");
    setBlobPath("");
  };

  const openTree = (nextPath: string) => {
    setPath(nextPath);
    setBlobPath("");
  };

  const refreshRepositories = async () => {
    await repositoriesQuery.refetch();
  };

  const loading =
    repositoriesQuery.isFetching ||
    refsQuery.isFetching ||
    treeQuery.isFetching ||
    blobQuery.isFetching;

  const error =
    repositoriesQuery.error?.message ||
    refsQuery.error?.message ||
    treeQuery.error?.message ||
    blobQuery.error?.message ||
    "";

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
            onClick={refreshRepositories}
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
          {!repositories.length && !repositoriesQuery.isFetching && (
            <p className="muted">No repositories yet.</p>
          )}
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
                  setSelectedRef(event.target.value);
                  setPath("");
                  setBlobPath("");
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
              <button type="button" onClick={() => openTree("")}>
                {selectedRepo.name}
              </button>
              {breadcrumbs.map((part, index) => {
                const nextPath = breadcrumbs.slice(0, index + 1).join("/");
                return (
                  <React.Fragment key={nextPath}>
                    <ChevronRight size={14} />
                    <button type="button" onClick={() => openTree(nextPath)}>
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
                        ? openTree(entry.path)
                        : setBlobPath(entry.path)
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
                {!entries.length && !treeQuery.isFetching && (
                  <p className="muted">Empty tree.</p>
                )}
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
