import React, { useEffect, useMemo, useState } from "react";
import {
  Check,
  ChevronRight,
  Clipboard,
  Code2,
  FileText,
  Folder,
  GitBranch,
  GitCommitHorizontal,
  Globe2,
  HardDrive,
  KeyRound,
  RefreshCw,
  Search,
  Server,
  ShieldCheck,
} from "lucide-react";
import type { GitRef, Repository, ServiceHealth } from "../server/originBackend";
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

function shortRefName(name: string): string {
  return name.replace("refs/heads/", "").replace("refs/tags/", "");
}

function shortOid(value: string): string {
  return value.length > 12 ? value.slice(0, 12) : value;
}

function cloneUrl(baseUrl: string, repo: Repository | null): string {
  if (!repo || !baseUrl) return "";
  return `${baseUrl}/${encodeURIComponent(repo.tenant)}/${encodeURIComponent(repo.name)}.git`;
}

function serviceIcon(service: ServiceHealth["name"]) {
  if (service === "gateway") return <ShieldCheck size={16} />;
  if (service === "webapp") return <Globe2 size={16} />;
  return <HardDrive size={16} />;
}

export default function Home() {
  const [selectedRepo, setSelectedRepo] = useState<Repository | null>(null);
  const [selectedRef, setSelectedRef] = useState("");
  const [path, setPath] = useState("");
  const [blobPath, setBlobPath] = useState("");
  const [repoFilter, setRepoFilter] = useState("");
  const [activeTab, setActiveTab] = useState<"code" | "refs" | "server">("code");
  const [copied, setCopied] = useState(false);

  const systemQuery = trpc.system.overview.useQuery(undefined, {
    staleTime: 3_000,
    refetchInterval: 15_000,
  });
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
      enabled: Boolean(selectedRepo && selectedRef && activeTab === "code"),
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
      enabled: Boolean(selectedRepo && selectedRef && blobPath && activeTab === "code"),
    },
  );

  const system = systemQuery.data;
  const repositories = repositoriesQuery.data?.repositories ?? [];
  const refs = refsQuery.data?.refs ?? [];
  const entries = treeQuery.data?.entries ?? [];
  const blob = blobQuery.data ?? null;
  const currentCloneUrl = cloneUrl(system?.cloneBaseUrl ?? "", selectedRepo);

  const selectedRepoKey = selectedRepo
    ? `${selectedRepo.tenant}/${selectedRepo.name}`
    : "";

  const branchRefs = useMemo(
    () => refs.filter((ref) => ref.name.startsWith("refs/heads/")),
    [refs],
  );
  const tagRefs = useMemo(
    () => refs.filter((ref) => ref.name.startsWith("refs/tags/")),
    [refs],
  );
  const visibleRepositories = useMemo(() => {
    const needle = repoFilter.trim().toLowerCase();
    if (!needle) return repositories;
    return repositories.filter((repo) =>
      `${repo.tenant}/${repo.name}`.toLowerCase().includes(needle),
    );
  }, [repoFilter, repositories]);

  useEffect(() => {
    if (selectedRepo || !repositories.length) return;
    setSelectedRepo(repositories[0]);
  }, [repositories, selectedRepo]);

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
    setActiveTab("code");
    setCopied(false);
  };

  const openTree = (nextPath: string) => {
    setPath(nextPath);
    setBlobPath("");
  };

  const refreshAll = async () => {
    const requests: Array<Promise<unknown>> = [systemQuery.refetch(), repositoriesQuery.refetch()];
    if (selectedRepo) requests.push(refsQuery.refetch());
    if (selectedRepo && selectedRef && activeTab === "code") requests.push(treeQuery.refetch());
    if (selectedRepo && selectedRef && blobPath && activeTab === "code") {
      requests.push(blobQuery.refetch());
    }
    await Promise.all(requests);
  };

  const copyCloneUrl = async () => {
    if (!currentCloneUrl) return;
    try {
      await navigator.clipboard.writeText(currentCloneUrl);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1_400);
    } catch {
      setCopied(false);
    }
  };

  const loading =
    systemQuery.isFetching ||
    repositoriesQuery.isFetching ||
    refsQuery.isFetching ||
    treeQuery.isFetching ||
    blobQuery.isFetching;

  const error =
    systemQuery.error?.message ||
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
            aria-label="Refresh"
            title="Refresh"
            onClick={refreshAll}
          >
            <RefreshCw size={16} />
          </button>
        </div>

        <div className="search-box">
          <Search size={15} />
          <input
            aria-label="Search repositories"
            value={repoFilter}
            onChange={(event) => setRepoFilter(event.target.value)}
            placeholder="Find a repository"
          />
        </div>

        <div className="repo-list">
          {visibleRepositories.map((repo) => {
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
          {!visibleRepositories.length && !repositoriesQuery.isFetching && (
            <p className="muted">No repositories.</p>
          )}
        </div>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div className="service-rail">
            {(system?.services ?? []).map((service) => (
              <div className={`service-pill ${service.status}`} key={service.name}>
                {serviceIcon(service.name)}
                <span>{service.label}</span>
                <small>{service.status}</small>
              </div>
            ))}
          </div>
          <button
            className="icon-button"
            type="button"
            aria-label="Refresh"
            title="Refresh"
            onClick={refreshAll}
          >
            <RefreshCw size={16} />
          </button>
        </header>

        <section className="repo-header">
          <div className="repo-title">
            <p className="eyebrow">Repository</p>
            <h1>
              {selectedRepo ? (
                <>
                  <span>{selectedRepo.tenant}</span>
                  <ChevronRight size={20} />
                  <strong>{selectedRepo.name}</strong>
                </>
              ) : (
                "Select a repository"
              )}
            </h1>
          </div>

          {selectedRepo && (
            <div className="repo-actions">
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
                      {shortRefName(ref.name)}
                    </option>
                  ))}
                  {refs
                    .filter((ref) => !ref.name.startsWith("refs/heads/"))
                    .map((ref) => (
                      <option key={ref.name} value={ref.name}>
                        {shortRefName(ref.name)}
                      </option>
                    ))}
                </select>
              </label>
            </div>
          )}
        </section>

        {selectedRepo && (
          <section className="clone-bar">
            <div>
              <KeyRound size={16} />
              <code>{currentCloneUrl}</code>
            </div>
            <button
              className="copy-button"
              type="button"
              onClick={copyCloneUrl}
              disabled={!currentCloneUrl}
              aria-label="Copy clone URL"
              title="Copy clone URL"
            >
              {copied ? <Check size={16} /> : <Clipboard size={16} />}
              <span>{copied ? "Copied" : "Copy"}</span>
            </button>
          </section>
        )}

        {error && <div className="error">{error}</div>}
        {loading && <div className="loading">Loading</div>}

        {selectedRepo && (
          <nav className="tabs" aria-label="Repository views">
            <button
              className={activeTab === "code" ? "active" : ""}
              type="button"
              onClick={() => setActiveTab("code")}
            >
              <Code2 size={16} />
              <span>Code</span>
            </button>
            <button
              className={activeTab === "refs" ? "active" : ""}
              type="button"
              onClick={() => setActiveTab("refs")}
            >
              <GitCommitHorizontal size={16} />
              <span>Refs</span>
            </button>
            <button
              className={activeTab === "server" ? "active" : ""}
              type="button"
              onClick={() => setActiveTab("server")}
            >
              <Server size={16} />
              <span>Server</span>
            </button>
          </nav>
        )}

        {selectedRepo && selectedRef && activeTab === "code" && (
          <>
            <div className="repo-stats">
              <span>{branchRefs.length} branches</span>
              <span>{tagRefs.length} tags</span>
              <span>{entries.length} entries</span>
            </div>

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
                    className={`file-row ${blobPath === entry.path ? "active" : ""}`}
                    key={entry.path}
                    type="button"
                    disabled={entry.kind !== "tree" && entry.kind !== "blob"}
                    title={entry.kind === "commit" ? "Submodule entry" : entry.kind}
                    onClick={() =>
                      entry.kind === "tree"
                        ? openTree(entry.path)
                        : entry.kind === "blob"
                          ? setBlobPath(entry.path)
                          : undefined
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

        {selectedRepo && activeTab === "refs" && (
          <section className="ref-table">
            {refs.map((ref) => (
              <div className="ref-row" key={ref.name}>
                <GitBranch size={16} />
                <span>{shortRefName(ref.name)}</span>
                <code>{shortOid(ref.target)}</code>
              </div>
            ))}
            {!refs.length && !refsQuery.isFetching && <p className="muted">No refs.</p>}
          </section>
        )}

        {selectedRepo && activeTab === "server" && (
          <section className="server-grid">
            <div className="server-panel">
              <h2>Gateway</h2>
              <p>{system?.services.find((service) => service.name === "gateway")?.detail}</p>
              <div className="backend-list">
                {(system?.gateway?.backends ?? []).map((backend) => (
                  <div className="backend-row" key={backend.url}>
                    <span>{backend.url}</span>
                    <small>{backend.status}</small>
                  </div>
                ))}
              </div>
            </div>
            <div className="server-panel">
              <h2>Webapp backend</h2>
              <p>{system?.services.find((service) => service.name === "webapp")?.detail}</p>
              <code>/api/trpc</code>
            </div>
            <div className="server-panel">
              <h2>Origin API / engine</h2>
              <p>{system?.services.find((service) => service.name === "origin")?.detail}</p>
              <code>/api/repos/{selectedRepo.tenant}/{selectedRepo.name}</code>
            </div>
            <div className="server-panel wide">
              <h2>Git access</h2>
              <p>{system?.gitAuthHint}</p>
              <code>git clone {currentCloneUrl}</code>
            </div>
          </section>
        )}
      </section>
    </main>
  );
}
