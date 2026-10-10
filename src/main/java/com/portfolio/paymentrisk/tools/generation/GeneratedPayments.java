package com.portfolio.paymentrisk.tools.generation;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.Json;
import java.util.*;

/** A seeded customer model, merged chronologically with O(customers) pending records. */
public final class GeneratedPayments implements Iterable<GeneratedPayments.Payment> {
  public static final String VERSION = "customer-generator-v1";

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
        boolean burst =
            Set.of("takeover", "card_testing", "low_burst").contains(customer.profile());
        long t =
            i == 0
                ? customer.start()
                : burst
                    ? customer.end() - 100000 + (i - 1) * 100000L / (customer.payments() - 2)
                    : customer.start()
                        + i * (customer.end() - customer.start()) / (customer.payments() - 1);
        long amount = random.nextLong(config.ordinaryMin(), config.ordinaryMax() + 1);
        String device = customer.id() + "-phone", status = "APPROVED";
        if (customer.profile().equals("takeover") && i > 0) {
          amount = random.nextLong(config.largeMin(), config.largeMax() + 1);
          device = customer.id() + "-device-" + i;
          status = i <= 4 ? "DECLINED" : "APPROVED";
        }
        if (customer.profile().equals("card_testing") && i > 0) {
          device = customer.id() + "-probe";
          amount =
              last
                  ? random.nextLong(config.largeMin(), config.largeMax() + 1)
                  : random.nextLong(50, 301);
          status = last ? "APPROVED" : "DECLINED";
        }
        if (customer.profile().equals("low_burst") && i > 0) amount = random.nextLong(100, 701);
        if (customer.profile().equals("new_device") && last) {
          device = customer.id() + "-replacement";
          amount = random.nextLong(config.largeMin(), config.largeMax() + 1);
        }
        next =
            Json.object()
                .put("customer_id", customer.id())
                .put("merchant_id", "merchant-" + random.nextInt(1, 41))
                .put("amount_minor", amount)
                .put("currency", "EUR")
                .put("country", List.of("IT", "FR", "DE", "ES").get(random.nextInt(4)))
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
