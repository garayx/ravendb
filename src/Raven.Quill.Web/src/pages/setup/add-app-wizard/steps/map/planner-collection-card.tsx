import { CircleAlert, Table2 } from "lucide-react";
import { Badge } from "@/components/shadcn/ui/badge";
import { Checkbox } from "@/components/shadcn/ui/checkbox";
import { CodeBlock, TranscriptDisclosure } from "@/components/chat/transcript-disclosure";
import { Text } from "@/components/typography";
import type { PlannerCollection } from "@/pages/setup/add-app-wizard/app-wizard-store";

export function PlannerCollectionCard({
    collection,
    isSelected,
    onSelectedChange,
}: {
    collection: PlannerCollection;
    isSelected: boolean;
    onSelectedChange: (isSelected: boolean) => void;
}) {
    const isRejected = collection.status === "rejected";

    return (
        <div className="grid gap-2">
            <div className="flex items-center gap-2">
                {/* A rejected attempt registered nothing, so there is nothing to carry forward. */}
                {!isRejected && (
                    <Checkbox
                        checked={isSelected}
                        onCheckedChange={(value) => onSelectedChange(Boolean(value))}
                        aria-label={`Include ${collection.collection} in the mapping`}
                    />
                )}
                <Text variant="label" as="span" className="min-w-0 flex-1 truncate">
                    {collection.collection}
                </Text>
                {isRejected ? (
                    <Badge variant="destructive">rejected</Badge>
                ) : (
                    <>
                        <Badge variant="secondary">v{collection.version}</Badge>
                        <Badge variant={collection.status === "replaced" ? "info" : "success"}>
                            {collection.status}
                        </Badge>
                    </>
                )}
            </div>

            {collection.rationale && (
                <Text variant="muted" className="pl-6">
                    {collection.rationale}
                </Text>
            )}

            {collection.errors && collection.errors.length > 0 && (
                <TranscriptDisclosure
                    disclosureKey={`planner-errors-${collection.collection}`}
                    icon={CircleAlert}
                    label={`${collection.errors.length} validation error(s)`}
                >
                    <div className="px-3 pb-3">
                        <ul className="grid gap-1">
                            {collection.errors.map((error) => (
                                <li key={error}>
                                    <Text variant="muted">{error}</Text>
                                </li>
                            ))}
                        </ul>
                    </div>
                </TranscriptDisclosure>
            )}

            {collection.warnings.length > 0 && (
                <ul className="grid gap-1 pl-6">
                    {collection.warnings.map((warning) => (
                        <li key={warning}>
                            <Text variant="caption">{warning}</Text>
                        </li>
                    ))}
                </ul>
            )}

            {collection.config !== undefined && (
                <TranscriptDisclosure
                    disclosureKey={`planner-config-${collection.collection}`}
                    icon={Table2}
                    label="Mapping"
                >
                    <div className="px-3 pb-3">
                        <CodeBlock value={JSON.stringify(collection.config, null, 2)} />
                    </div>
                </TranscriptDisclosure>
            )}
        </div>
    );
}
