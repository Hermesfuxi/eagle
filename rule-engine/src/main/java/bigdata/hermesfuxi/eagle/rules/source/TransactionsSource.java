package bigdata.hermesfuxi.eagle.rules.source;

import java.util.Properties;

import bigdata.hermesfuxi.eagle.rules.config.Config;
import bigdata.hermesfuxi.eagle.rules.config.Transaction;
import bigdata.hermesfuxi.eagle.rules.dynamicrules.functions.JsonDeserializer;
import bigdata.hermesfuxi.eagle.rules.dynamicrules.functions.JsonGeneratorWrapper;
import bigdata.hermesfuxi.eagle.rules.dynamicrules.functions.TimeStamper;
import bigdata.hermesfuxi.eagle.rules.dynamicrules.functions.TransactionsGenerator;
import bigdata.hermesfuxi.eagle.rules.utils.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import static bigdata.hermesfuxi.eagle.rules.config.Parameters.*;

public class TransactionsSource {

  public static SourceFunction<String> createTransactionsSource(Config config) {

    String sourceType = config.get(TRANSACTIONS_SOURCE);
    Type transactionsSourceType =
        Type.valueOf(sourceType.toUpperCase());

    int transactionsPerSecond = config.get(RECORDS_PER_SECOND);

    switch (transactionsSourceType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
        String transactionsTopic = config.get(DATA_TOPIC);
        FlinkKafkaConsumer<String> kafkaConsumer =
            new FlinkKafkaConsumer<>(transactionsTopic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
      default:
        return new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));
    }
  }

  public static DataStream<Transaction> stringsStreamToTransactions(
      DataStream<String> transactionStrings) {
    return transactionStrings
        .flatMap(new JsonDeserializer<Transaction>(Transaction.class))
        .returns(Transaction.class)
        .flatMap(new TimeStamper<Transaction>())
        .returns(Transaction.class)
        .name("Transactions Deserialization");
  }

  public enum Type {
    GENERATOR("Transactions Source (generated locally)"),
    KAFKA("Transactions Source (Kafka)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
