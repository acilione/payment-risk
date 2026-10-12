export const profiles: Record<string, { name: string; description: string }> = {
  normal: {
    name: "Regular purchases",
    description:
      "Everyday purchases from a familiar phone, spread across the day.",
  },
  known_devices: {
    name: "Familiar phone and laptop",
    description: "An established phone and laptop used for ordinary purchases.",
  },
  checkout_retry: {
    name: "Checkout retry",
    description:
      "A declined checkout followed by a fresh attempt at the same merchant and amount.",
  },
  new_device: {
    name: "Replacement phone",
    description: "A legitimate larger purchase after replacing a phone.",
  },
  takeover: {
    name: "Account takeover",
    description:
      "Three large purchases from one unfamiliar device after ordinary activity.",
  },
  card_testing: {
    name: "Card testing",
    description:
      "Small declined attempts followed by a larger purchase on the same device.",
  },
  low_burst: {
    name: "Small payment burst",
    description:
      "Several small payments in quick succession on a familiar device.",
  },
  trusted_device: {
    name: "Familiar device misuse",
    description:
      "Simulated misuse whose payment facts resemble ordinary activity.",
  },
};
