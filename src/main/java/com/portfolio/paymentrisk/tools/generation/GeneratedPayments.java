package com.portfolio.paymentrisk.tools.generation;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.Json;
import java.util.*;

/** A seeded customer model, merged chronologically with O(customers) pending records. */
public final class GeneratedPayments implements Iterable<GeneratedPayments.Payment> {
  public static final String VERSION = "customer-generator-v2";

  public record Customer(
      String id, String profile, int payments, long start, long end, long seed) {}

  public record Payment(ObjectNode value, boolean retry, boolean target) {}

  private final GeneratorConfig config;
  private final String run;
  private final List<Customer> customers;

  public GeneratedPayments(GeneratorConfig config, String run, long end) {
    if (!run.matches("show-[a-zA-Z0-9-]{1,60}") || end <= config.historyMinutes() * 60000L)
      throw new IllegalArgumentException("Invalid run or event-time anchor");
    this.config = config;
    this.run = run;
    var random = new SplittableRandom(config.seed());
    var plans = new ArrayList<Customer>();
    for (int i = 0; i < config.customers(); i++) {
      int choice = random.nextInt(100);
      String profile = "normal";
      for (var entry : config.profiles().entrySet()) {
        choice -= entry.getValue();
        if (choice < 0) {
          profile = entry.getKey();
          break;
        }
      }
      plans.add(
          new Customer(
              run + String.format("-customer-%04d", i),
              profile,
              random.nextInt(config.minPayments(), config.maxPayments() + 1),
              end - config.historyMinutes() * 60000L + random.nextInt(30000),
              end - random.nextInt(10000),
              random.nextLong()));
    }
    customers = List.copyOf(plans);
  }

  public List<Customer> customers() {
    return customers;
  }

  public int size() {
    return customers.stream().mapToInt(Customer::payments).sum();
  }

  private long ordinaryAmount(SplittableRandom random) {
    // Everyday purchases are concentrated toward the lower end of the configured range.
    return Math.min(
        random.nextLong(config.ordinaryMin(), config.ordinaryMax() + 1),
        random.nextLong(config.ordinaryMin(), config.ordinaryMax() + 1));
  }

  public Iterator<Payment> iterator() {
    class Cursor {
      final Customer customer;
      final SplittableRandom random;
      int index;
      ObjectNode next;

      Cursor(Customer c) {
        customer = c;
        random = new SplittableRandom(c.seed());
        advance();
      }

      void advance() {
        if (index >= customer.payments()) {
          next = null;
          return;
        }
        int i = index++;
        boolean last = i == customer.payments() - 1;
        // Only the final sequence changes behavior; earlier purchases establish a baseline.
        int tail =
            switch (customer.profile()) {
              case "takeover" -> 3;
              case "card_testing", "low_burst" -> 5;
              case "checkout_retry" -> 2;
              default -> 0;
            };
        int baseline = customer.payments() - tail;
        boolean inSequence = tail > 0 && i >= baseline;
        var habits = new SplittableRandom(customer.seed());
        String country = List.of("IT", "FR", "DE", "ES").get(habits.nextInt(4));
        int merchantBase = habits.nextInt(1, 31);
        long sequenceMs =
            switch (customer.profile()) {
              case "takeover" -> habits.nextLong(180000, 360001);
              case "card_testing" -> habits.nextLong(60000, 110001);
              case "low_burst" -> habits.nextLong(45000, 90001);
              case "checkout_retry" -> habits.nextLong(45000, 120001);
              default -> 0;
            };
        sequenceMs = Math.min(sequenceMs, (customer.end() - customer.start()) / 3);
        // Baseline ends before the final sequence; jitter stays inside non-overlapping slots.
        long baselineEnd =
            tail > 0
                ? Math.max(customer.start() + 1, customer.end() - sequenceMs - 60000)
                : customer.end();
        long slot = (baselineEnd - customer.start()) / Math.max(1, baseline - 1);
        long t =
            inSequence
                ? customer.end() - sequenceMs + (i - baseline) * sequenceMs / (tail - 1)
                : customer.start() + i * slot;
        if (!inSequence && i > 0 && i < baseline - 1) t -= random.nextLong(Math.max(1, slot / 3));
        long amount = ordinaryAmount(random);
        String device = customer.id() + "-phone", status = "APPROVED";
        String merchant = "merchant-" + country + "-" + (merchantBase + random.nextInt(4));
        if (customer.profile().equals("known_devices") && i % 3 == 1)
          device = customer.id() + "-laptop";
        if (customer.profile().equals("takeover") && inSequence) {
          amount = random.nextLong(config.largeMin(), config.largeMax() + 1);
          device = customer.id() + "-unfamiliar";
          merchant = "merchant-" + country + "-electronics";
        }
        if (customer.profile().equals("card_testing") && inSequence) {
          device = customer.id() + "-unfamiliar";
          merchant = "merchant-" + country + "-online-" + ((i - baseline) % 2);
          amount =
              last
                  ? random.nextLong(config.largeMin(), config.largeMax() + 1)
                  : random.nextLong(50, 301);
          status = last ? "APPROVED" : "DECLINED";
        }
        if (customer.profile().equals("low_burst") && inSequence)
          amount = random.nextLong(100, 701);
        if (customer.profile().equals("checkout_retry") && inSequence) {
          // A fresh authorization attempt is a new transaction, unlike a transport redelivery.
          amount = ordinaryAmount(new SplittableRandom(customer.seed() ^ 0x1234L));
          merchant = "merchant-" + country + "-" + merchantBase;
          status = last ? "APPROVED" : "DECLINED";
        }
        if (customer.profile().equals("new_device") && last) {
          device = customer.id() + "-replacement";
          amount = random.nextLong(config.largeMin(), config.largeMax() + 1);
        }
        next =
            Json.object()
                .put("customer_id", customer.id())
                .put("merchant_id", merchant)
                .put("amount_minor", amount)
                .put("currency", "EUR")
                .put("country", country)
                .put("device_id", device)
                .put("status", status)
                .put("event_time", t)
                .put("producer_time", t);
      }
    }
    var queue =
        new PriorityQueue<Cursor>(
            Comparator.comparingLong((Cursor c) -> c.next.path("event_time").asLong())
                .thenComparing(c -> c.customer.id()));
    customers.forEach(c -> queue.add(new Cursor(c)));
    var retries = new SplittableRandom(config.seed() ^ 0x5deece66dL);
    return new Iterator<>() {
      long sequence;

      public boolean hasNext() {
        return !queue.isEmpty();
      }

      public Payment next() {
        if (!hasNext()) throw new NoSuchElementException();
        var c = queue.remove();
        var tx = c.next;
        boolean target = c.index == c.customer.payments();
        String id = run + String.format("-payment-%06d", sequence++);
        tx.put("event_id", id).put("transaction_id", "txn-" + id);
        c.advance();
        if (c.next != null) queue.add(c);
        return new Payment(tx, retries.nextInt(100) < config.retryPercent(), target);
      }
    };
  }
}
