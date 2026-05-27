import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

export type BootstrapService = ServerApi["bootstrap"];

export function createBootstrapQueries(api: BootstrapService) {
    return {
        status: () =>
            queryOptions({
                queryKey: ["bootstrap", "status"],
                queryFn: () => api.status(),
            }),
    };
}

export type BootstrapQueries = ReturnType<typeof createBootstrapQueries>;
