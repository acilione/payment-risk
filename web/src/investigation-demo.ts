import fixture from "./investigation-fixture.json";
import type {
  CustomerSummary,
  InvestigationDecision,
  CustomerPage,
  TimelinePage,
} from "./investigation-types";
export const stories = fixture.cases;
export const demoDecisions = stories.flatMap((story) =>
  story.timeline.map((entry) => entry.decision),
);
export const demoEventTime = Math.max(
  ...demoDecisions.map((decision) => decision.event_time),
);
const generatedAt = "2026-10-09T10:00:20.000Z";
const summary = (story: (typeof stories)[number]): CustomerSummary => ({
  customer_id: story.target_payment.customer_id,
  payments: story.timeline.length,
  alerts: story.timeline.filter((x) => x.decision.decision !== "APPROVE")
    .length,
  maxScore: Math.max(...story.timeline.map((x) => x.decision.risk_score)),
  lastEvent: story.target_payment.event_time,
});
export function demoCustomers(q: string, decision: string): CustomerPage {
  return {
    generatedAt,
    nextCursor: null,
    items: stories
      .filter(
        (s) =>
          s.target_payment.customer_id.startsWith(q) &&
          (decision === "ALL" ||
            s.timeline.some((x) => x.decision.decision === decision)),
      )
      .map(summary)
      .sort((a, b) => a.customer_id.localeCompare(b.customer_id)),
  };
}
export function demoTimeline(customer: string): TimelinePage {
  const story = stories.find((s) => s.target_payment.customer_id === customer);
  return {
    generatedAt,
    nextCursor: null,
    customer: story ? summary(story) : null,
    items: story
      ? story.timeline
          .map((x) => x.decision as InvestigationDecision)
          .sort(
            (a, b) =>
              b.event_time - a.event_time ||
              b.evaluation_id.localeCompare(a.evaluation_id),
          )
      : [],
  };
}
