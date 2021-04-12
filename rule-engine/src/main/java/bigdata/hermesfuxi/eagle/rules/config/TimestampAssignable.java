package bigdata.hermesfuxi.eagle.rules.config;

public interface TimestampAssignable<T> {
  void assignIngestionTimestamp(T timestamp);
}
