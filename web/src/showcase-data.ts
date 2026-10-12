import snapshot from "./showcase-data.json";

// Store repeated policy text once in the portable export; restore the exact stored text.
const policies: Record<string, string> = snapshot.policies;
const cases = snapshot.cases.map((story) => ({
  ...story,
  decision: {
    ...story.decision,
    policy_snapshot: policies[story.decision.policy_ref],
  },
  timeline: story.timeline.map((entry) => ({
    ...entry,
    decision: {
      ...entry.decision,
      policy_snapshot: policies[entry.decision.policy_ref],
    },
  })),
}));
export default { ...snapshot, cases };
