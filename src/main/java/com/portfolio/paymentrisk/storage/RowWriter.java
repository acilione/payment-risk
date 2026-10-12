package com.portfolio.paymentrisk.storage;

import java.io.IOException;

/** Persists a bounded unit before its consumer acknowledges any source offsets. */
public interface RowWriter {
  void insert(String table, String rows) throws IOException, InterruptedException;
}
