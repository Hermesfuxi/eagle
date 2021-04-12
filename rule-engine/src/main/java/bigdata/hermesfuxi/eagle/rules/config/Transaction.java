package bigdata.hermesfuxi.eagle.rules.config;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class Transaction implements TimestampAssignable<Long> {
  public long transactionId;
  public long eventTime;
  public long payeeId;
  public long beneficiaryId;
  public BigDecimal paymentAmount;
  public PaymentType paymentType;
  private Long ingestionTimestamp;

  public Transaction() {
  }

  public Transaction(long transactionId, long eventTime, long payeeId, long beneficiaryId, BigDecimal paymentAmount, PaymentType paymentType, Long ingestionTimestamp) {
    this.transactionId = transactionId;
    this.eventTime = eventTime;
    this.payeeId = payeeId;
    this.beneficiaryId = beneficiaryId;
    this.paymentAmount = paymentAmount;
    this.paymentType = paymentType;
    this.ingestionTimestamp = ingestionTimestamp;
  }

  private static transient DateTimeFormatter timeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
          .withLocale(Locale.US)
          .withZone(ZoneOffset.UTC);

  public enum PaymentType {
    CSH("CSH"),
    CRD("CRD");

    String representation;

    PaymentType(String repr) {
      this.representation = repr;
    }

    public static PaymentType fromString(String representation) {
      for (PaymentType b : PaymentType.values()) {
        if (b.representation.equals(representation)) {
          return b;
        }
      }
      return null;
    }
  }

  public static Transaction fromString(String line) {
    List<String> tokens = Arrays.asList(line.split(","));
    int numArgs = 7;
    if (tokens.size() != numArgs) {
      throw new RuntimeException(
          "Invalid transaction: "
              + line
              + ". Required number of arguments: "
              + numArgs
              + " found "
              + tokens.size());
    }

    Transaction transaction = new Transaction();

    try {
      Iterator<String> iter = tokens.iterator();
      transaction.transactionId = Long.parseLong(iter.next());
      transaction.eventTime =
          ZonedDateTime.parse(iter.next(), timeFormatter).toInstant().toEpochMilli();
      transaction.payeeId = Long.parseLong(iter.next());
      transaction.beneficiaryId = Long.parseLong(iter.next());
      transaction.paymentType = PaymentType.fromString(iter.next());
      transaction.paymentAmount = new BigDecimal(iter.next());
      transaction.ingestionTimestamp = Long.parseLong(iter.next());
    } catch (NumberFormatException nfe) {
      throw new RuntimeException("Invalid record: " + line, nfe);
    }

    return transaction;
  }

  @Override
  public void assignIngestionTimestamp(Long timestamp) {
    this.ingestionTimestamp = timestamp;
  }

  public long getTransactionId() {
    return transactionId;
  }

  public void setTransactionId(long transactionId) {
    this.transactionId = transactionId;
  }

  public long getEventTime() {
    return eventTime;
  }

  public void setEventTime(long eventTime) {
    this.eventTime = eventTime;
  }

  public long getPayeeId() {
    return payeeId;
  }

  public void setPayeeId(long payeeId) {
    this.payeeId = payeeId;
  }

  public long getBeneficiaryId() {
    return beneficiaryId;
  }

  public void setBeneficiaryId(long beneficiaryId) {
    this.beneficiaryId = beneficiaryId;
  }

  public BigDecimal getPaymentAmount() {
    return paymentAmount;
  }

  public void setPaymentAmount(BigDecimal paymentAmount) {
    this.paymentAmount = paymentAmount;
  }

  public PaymentType getPaymentType() {
    return paymentType;
  }

  public void setPaymentType(PaymentType paymentType) {
    this.paymentType = paymentType;
  }

  public Long getIngestionTimestamp() {
    return ingestionTimestamp;
  }

  public void setIngestionTimestamp(Long ingestionTimestamp) {
    this.ingestionTimestamp = ingestionTimestamp;
  }

  public static DateTimeFormatter getTimeFormatter() {
    return timeFormatter;
  }

  public static void setTimeFormatter(DateTimeFormatter timeFormatter) {
    Transaction.timeFormatter = timeFormatter;
  }
}
