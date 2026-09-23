import { useRef, useState } from "react";
import { useFormContext } from "react-hook-form";
import { api } from "@/api/api";
import type { MigrationFrame } from "@/api/custom-services/migration-service";
import { Button } from "@/components/shadcn/ui/button";
import { Textarea } from "@/components/shadcn/ui/textarea";
import { Heading, Text } from "@/components/typography";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { computeSourceKey } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { computeMapKey } from "@/pages/setup/add-app-wizard/steps/map/use-map-schema-step";
import { useApplyMapTables } from "@/pages/setup/add-app-wizard/steps/map-tables/use-apply-map-tables";
import { wrapDtoTablesToFormShape } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import { scaffoldTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { tablesSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";

/**
 * A developer view of the interactive planner, not a designed screen: it shows the raw frames the
 * agent produced so the conversation can be driven by hand while the backend is being built. The
 * real UI is a separate piece of work.
 */
export function DesignWithAiStep() {
    const { getValues, setValue } = useFormContext<AppFormData>();
    const applyMapTables = useApplyMapTables();

    const [log, setLog] = useState<string[]>([]);
    const [conversationId, setConversationId] = useState<string | null>(null);
    const [prompt, setPrompt] = useState("");
    const [isStreaming, setIsStreaming] = useState(false);
    const abortRef = useRef<AbortController | null>(null);

    const append = (entry: string) => setLog((entries) => [...entries, entry]);

    const consume = async (frames: AsyncGenerator<MigrationFrame>) => {
        for await (const frame of frames) {
            append(JSON.stringify(frame));

            if (frame.type === "done") {
                setConversationId(frame.conversationId);
            }
        }
    };

    const run = async (frames: (signal: AbortSignal) => AsyncGenerator<MigrationFrame>) => {
        const controller = new AbortController();
        abortRef.current = controller;
        setIsStreaming(true);

        try {
            await consume(frames(controller.signal));
        } catch (error) {
            if (!controller.signal.aborted) {
                append(`ERROR ${error instanceof Error ? error.message : String(error)}`);
            }
        } finally {
            abortRef.current = null;
            setIsStreaming(false);
        }
    };

    const slug = () => getValues("externalConnection").slug;

    const handleStart = () =>
        run((signal) =>
            api.services.migration.start(
                { slug: slug(), selectedTables: getValues("verifySchema").tables },
                signal,
            ),
        );

    const handleAsk = () => {
        if (!conversationId) {
            return;
        }

        const text = prompt;
        setPrompt("");

        return run((signal) => api.services.migration.ask({ slug: slug(), conversationId, prompt: text }, signal));
    };

    const handleApply = async () => {
        if (!conversationId) {
            return;
        }

        try {
            const result = await api.services.migration.apply({ slug: slug(), conversationId });

            append(`APPLY unmapped=${JSON.stringify(result.unmappedTables)} errors=${JSON.stringify(result.errors)}`);

            if (result.configuration) {
                adoptTables(tablesSchema.parse(wrapDtoTablesToFormShape(result.configuration.tables ?? [])));
            }
        } catch (error) {
            append(`APPLY FAILED ${error instanceof Error ? error.message : String(error)}`);
        }
    };

    const handleManual = () => {
        const store = useSetupWizardStore.getState();
        adoptTables(scaffoldTables(getValues("verifySchema").tables, store.discoverResult));
        append("Scaffolded a manual mapping.");
    };

    /** Whatever produced the tables, mapTables only renders them once they are the applied answer. */
    const adoptTables = (tables: AppFormData["mapTables"]["tables"]) => {
        const store = useSetupWizardStore.getState();

        setValue("map.source", "manual");
        applyMapTables(tables);
        store.setAppliedMapKey(
            computeMapKey({
                sourceKey: computeSourceKey(getValues("externalConnection")),
                source: "manual",
                aiPrompt: "",
                selectedTables: getValues("verifySchema").tables,
            }),
        );
        store.resetMapTablesUiState();
    };

    return (
        <div className="flex flex-col gap-4">
            <div>
                <Heading as="h3">Design with AI (developer view)</Heading>
                <Text variant="muted">
                    Raw planner frames. The designed screen is a separate piece of work.
                </Text>
            </div>

            <div className="flex flex-wrap items-center gap-2">
                <Button type="button" onClick={handleStart} disabled={isStreaming}>
                    Start
                </Button>
                <Button type="button" variant="outline" onClick={handleApply} disabled={!conversationId || isStreaming}>
                    Apply to mapping
                </Button>
                <Button type="button" variant="outline" onClick={handleManual} disabled={isStreaming}>
                    Use manual mapping
                </Button>
                <Text variant="muted">{conversationId ?? "no conversation yet"}</Text>
            </div>

            <div className="flex flex-col gap-2">
                <Textarea
                    rows={3}
                    value={prompt}
                    onChange={(event) => setPrompt(event.target.value)}
                    placeholder="go ahead with orders and products, and I need lines of credit per customer"
                />
                <div>
                    <Button
                        type="button"
                        onClick={handleAsk}
                        disabled={!conversationId || isStreaming || !prompt.trim()}
                    >
                        Send
                    </Button>
                </div>
            </div>

            <pre className="max-h-96 overflow-auto rounded border p-2 text-xs whitespace-pre-wrap">
                {log.join("\n\n")}
            </pre>
        </div>
    );
}
