export const ruleCatalog = [
  {
    id: "R001",
    name: "Payment frequency",
    description: "More than 5 payments by the same customer within 2 minutes.",
    score: 25,
  },
  {
    id: "R002",
    name: "Total payment amount",
    description:
      "More than €3,000 in payments by the same customer within 10 minutes.",
    score: 35,
  },
  {
    id: "R003",
    name: "Multiple devices",
    description:
      "Payments from at least 3 devices for the same customer within 15 minutes.",
    score: 20,
  },
  {
    id: "R004",
    name: "New device",
    description:
      "A payment of at least €800 from a device not seen for that customer in the previous 30 days.",
    score: 30,
  },
  {
    id: "R005",
    name: "Approval after declines",
    description:
      "An approved payment after at least 4 declines for the same customer within 10 minutes.",
    score: 40,
  },
];
