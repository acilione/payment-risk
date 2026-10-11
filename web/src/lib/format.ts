import type { DecisionKind } from "../types";
const numberFormatter = new Intl.NumberFormat("en-GB");
const moneyFormatter = new Intl.NumberFormat("en-IE", {
  style: "currency",
  currency: "EUR",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const timeFormatter = new Intl.DateTimeFormat("en-GB", {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});
const utcFormatter = new Intl.DateTimeFormat("en-GB", {
  timeZone: "UTC",
  day: "2-digit",
  month: "short",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});
export const formatNumber = (value: number) => numberFormatter.format(value);
export const formatMoney = (minorUnits: number) =>
  moneyFormatter.format(minorUnits / 100);
export const formatTime = (timestamp: number) =>
  timeFormatter.format(timestamp);
export const formatUtcDate = (timestamp: number) =>
  `${utcFormatter.format(timestamp)} UTC`;
export const shortenId = (id: string) =>
  id.length > 24 ? `${id.slice(0, 12)}…${id.slice(-7)}` : id;
export const decisionLabels: Record<DecisionKind, string> = {
  APPROVE: "Approved",
  REVIEW: "Review",
  REJECT: "Rejected",
};
