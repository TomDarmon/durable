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

export type ServiceHealth = {
  name: "gateway" | "webapp" | "origin";
  label: string;
  status: "ready" | "degraded";
  detail: string;
};

export type GatewayBackend = {
  url: string;
  status: string;
};

export type GatewayStatus = {
  ready: boolean;
  backends: GatewayBackend[];
};

export type SystemOverview = {
  services: ServiceHealth[];
  gateway: GatewayStatus | null;
  cloneBaseUrl: string;
  gitAuthHint: string;
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

const gatewayBaseUrl = (): string =>
  (process.env.ORIGIN_GATEWAY_URL || "http://127.0.0.1:9400").replace(/\/$/, "");

export const publicGitBaseUrl = (): string =>
  (
    process.env.ORIGIN_WEBAPP_PUBLIC_GIT_BASE_URL ||
    process.env.ORIGIN_GATEWAY_PUBLIC_URL ||
    gatewayBaseUrl()
  ).replace(/\/$/, "");

export const gitAuthHint = (): string =>
  process.env.ORIGIN_WEBAPP_GIT_AUTH_HINT || "Use a gateway token as your Git password.";

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

export async function gatewayJson<T>(path: string): Promise<T> {
  const headers: Record<string, string> = {
    accept: "application/json",
  };
  if (process.env.ORIGIN_GATEWAY_ADMIN_TOKEN) {
    headers.authorization = `Bearer ${process.env.ORIGIN_GATEWAY_ADMIN_TOKEN}`;
  }
  const response = await fetch(`${gatewayBaseUrl()}${path}`, {
    headers,
    cache: "no-store",
  });
  if (!response.ok) {
    const message = await response.text();
    throw new OriginBackendError(
      response.status,
      message || `Origin gateway returned HTTP ${response.status}`,
    );
  }
  return response.json();
}

export async function repositoryListJson(): Promise<RepositoriesResponse> {
  if (process.env.ORIGIN_GATEWAY_ADMIN_TOKEN) {
    return gatewayJson<RepositoriesResponse>("/admin/repos");
  }
  return backendJson<RepositoriesResponse>("/api/repos");
}

export async function serviceReady(path: string): Promise<boolean> {
  try {
    const response = await fetch(`${backendBaseUrl()}${path}`, { cache: "no-store" });
    return response.ok || response.status === 204;
  } catch {
    return false;
  }
}

export function backendErrorStatus(error: unknown): number | undefined {
  return error instanceof OriginBackendError ? error.status : undefined;
}

export function repositoryPath(tenant: string, name: string, suffix: string): string {
  return `/api/repos/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}${suffix}`;
}
