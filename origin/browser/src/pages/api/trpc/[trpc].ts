import { createNextApiHandler } from "@trpc/server/adapters/next";
import { appRouter, createContext } from "../../../server/trpc";

export default createNextApiHandler({
  router: appRouter,
  createContext,
  onError({ error, path }) {
    console.error("trpc request failed", {
      path,
      code: error.code,
      message: error.message,
    });
  },
});
