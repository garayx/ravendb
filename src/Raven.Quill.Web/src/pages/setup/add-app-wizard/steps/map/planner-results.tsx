import { ListTree } from "lucide-react";
import { CodeBlock, TranscriptDisclosure } from "@/components/chat/transcript-disclosure";
import { Separator } from "@/components/shadcn/ui/separator";
import { Heading, Text } from "@/components/typography";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { PlannerCollectionCard } from "@/pages/setup/add-app-wizard/steps/map/planner-collection-card";

/** What the planner has actually registered, as opposed to what it said it would. */
export function PlannerResults() {
    const proposal = useSetupWizardStore((state) => state.plannerProposal);
    const collections = useSetupWizardStore((state) => state.plannerCollections);
    const deselected = useSetupWizardStore((state) => state.plannerDeselected);
    const toggle = useSetupWizardStore((state) => state.togglePlannerCollection);

    const registered = Object.values(collections);

    if (!proposal && registered.length === 0) {
        return (
            <div className="flex h-full items-center justify-center p-6">
                <Text variant="muted">Nothing registered yet.</Text>
            </div>
        );
    }

    return (
        <div className="flex h-full min-h-0 flex-col gap-4 overflow-y-auto p-3">
            {proposal && (
                <TranscriptDisclosure disclosureKey="planner-proposal" icon={ListTree} label="Proposed plan">
                    <div className="px-3 pb-3">
                        <CodeBlock value={JSON.stringify(proposal, null, 2)} className="max-h-96" />
                    </div>
                </TranscriptDisclosure>
            )}

            {registered.length > 0 && (
                <div className="grid gap-3">
                    <Heading as="h3" variant="subsection">
                        Collections
                    </Heading>
                    {registered.map((collection, index) => (
                        <div key={collection.collection} className="grid gap-3">
                            {index > 0 && <Separator />}
                            <PlannerCollectionCard
                                collection={collection}
                                isSelected={!deselected[collection.collection]}
                                onSelectedChange={(isSelected) => toggle(collection.collection, isSelected)}
                            />
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}
