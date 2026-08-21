import { initTRPC, TRPCError } from "@trpc/server";
import { z } from "zod";
import {
  backendErrorStatus,
  backendJson,
  type BlobResponse,
  type RefsResponse,
  type RepositoriesResponse,
  repositoryPath,
  type TreeResponse,
} from "./originBackend";

const t = initTRPC.create();

const repositoryInput = z.object({
  tenant: z.string().min(1),
  name: z.string().min(1),
});

const treeInput = repositoryInput.extend({
  reference: z.string().min(1),
  path: z.string().default(""),
});

const blobInput = repositoryInput.extend({
  reference: z.string().min(1),
  path: z.string().min(1),
});

async function fromBackend<T>(call: () => Promise<T>): Promise<T> {
  try {
    return await call();
  } catch (error) {
    const status = backendErrorStatus(error);
    console.error("origin ui api request failed", {
      status,
      message: error instanceof Error ? error.message : String(error),
    });
    throw new TRPCError({
      code: status === 404 ? "NOT_FOUND" : "INTERNAL_SERVER_ERROR",
      message: error instanceof Error ? error.message : String(error),
      cause: error,
    });
  }
}

export const appRouter = t.router({
  repositories: t.router({
    list: t.procedure.query(() =>
      fromBackend(() => backendJson<RepositoriesResponse>("/api/repos")),
    ),
    refs: t.procedure.input(repositoryInput).query(({ input }) =>
      fromBackend(() =>
        backendJson<RefsResponse>(repositoryPath(input.tenant, input.name, "/refs")),
      ),
    ),
    tree: t.procedure.input(treeInput).query(({ input }) => {
      const query = new URLSearchParams({ ref: input.reference });
      if (input.path) query.set("path", input.path);
      return fromBackend(() =>
        backendJson<TreeResponse>(
          `${repositoryPath(input.tenant, input.name, "/tree")}?${query.toString()}`,
        ),
      );
    }),
    blob: t.procedure.input(blobInput).query(({ input }) => {
      const query = new URLSearchParams({
        ref: input.reference,
        path: input.path,
      });
      return fromBackend(() =>
        backendJson<BlobResponse>(
          `${repositoryPath(input.tenant, input.name, "/blob")}?${query.toString()}`,
        ),
      );
    }),
  }),
});

export function createContext() {
  return {};
}

export type AppRouter = typeof appRouter;
