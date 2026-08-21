export type Repository = {
  tenant: string;
  name: string;
};

export type GitRef = {
  name: string;
  target: string;
};

export type TreeEntry = {
  name: string;
  path: string;
  kind: "blob" | "commit" | "tag" | "tree";
  oid: string;
  size: number | null;
};

export type RepositoriesResponse = {
  repositories: Repository[];
};

export type RefsResponse = {
  refs: GitRef[];
};

export type TreeResponse = {
  reference: string;
  path: string;
  entries: TreeEntry[];
};

export type BlobResponse = {
  reference: string;
  path: string;
  content: string;
};

class OriginBackendError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "OriginBackendError";
    this.status = status;
  }
}

const backendBaseUrl = (): string =>
  (process.env.ORIGIN_UI_BACKEND_URL || "http://127.0.0.1:9210").replace(/\/$/, "");

export async function backendJson<T>(path: string): Promise<T> {
  const response = await fetch(`${backendBaseUrl()}${path}`, {
    headers: {
      accept: "application/json",
    },
    cache: "no-store",
  });
  if (!response.ok) {
    const message = await response.text();
    throw new OriginBackendError(
      response.status,
      message || `Origin UI API returned HTTP ${response.status}`,
    );
  }
  return response.json();
}

export function backendErrorStatus(error: unknown): number | undefined {
  return error instanceof OriginBackendError ? error.status : undefined;
}

export function repositoryPath(tenant: string, name: string, suffix: string): string {
  return `/api/repos/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}${suffix}`;
}
