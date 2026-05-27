import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

export type AppsService = ServerApi["apps"];

export function createAppsQueries(api: AppsService) {
    return {
        detail: (appId: string) =>
            queryOptions({
                queryKey: ["apps", "detail", appId],
                queryFn: () => api.detail(appId),
            }),
        list: () =>
            queryOptions({
                queryKey: ["apps", "list"],
                queryFn: () => api.list(),
            }),
    };
}

export type AppsQueries = ReturnType<typeof createAppsQueries>;
