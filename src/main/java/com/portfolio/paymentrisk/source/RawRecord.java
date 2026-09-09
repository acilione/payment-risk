package com.portfolio.paymentrisk.source;

public class RawRecord {
  public String topic;
  public int partition;
  public long offset;
  public long timestamp;
  public byte[] payload;
  public String decoded;
  public String validationError;
  public long eventTime = Long.MIN_VALUE;

  public RawRecord() {}

  public RawRecord(String topic, int partition, long offset, long timestamp, byte[] payload) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.payload = payload;
  }
}
