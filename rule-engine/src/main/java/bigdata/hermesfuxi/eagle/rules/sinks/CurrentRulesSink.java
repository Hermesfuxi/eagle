package bigdata.hermesfuxi.eagle.rules.sinks;

import java.io.IOException;
import java.util.Properties;

import bigdata.hermesfuxi.eagle.rules.config.Config;
import bigdata.hermesfuxi.eagle.rules.dynamicrules.functions.JsonSerializer;
import bigdata.hermesfuxi.eagle.rules.pojo.RulePojo;
import bigdata.hermesfuxi.eagle.rules.utils.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import static bigdata.hermesfuxi.eagle.rules.config.Parameters.RULES_EXPORT_SINK;
import static bigdata.hermesfuxi.eagle.rules.config.Parameters.RULES_EXPORT_TOPIC;

public class CurrentRulesSink {

  public static SinkFunction<String> createRulesSink(Config config) throws IOException {

    String sinkType = config.get(RULES_EXPORT_SINK);
    Type currentRulesSinkType =
        Type.valueOf(sinkType.toUpperCase());

    switch (currentRulesSinkType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initProducerProperties(config);
        String alertsTopic = config.get(RULES_EXPORT_TOPIC);
        return new FlinkKafkaProducer<>(alertsTopic, new SimpleStringSchema(), kafkaProps);
      case STDOUT:
        return new PrintSinkFunction<>(true);
      default:
        throw new IllegalArgumentException(
            "Source \"" + currentRulesSinkType + "\" unknown. Known values are:" + Type.values());
    }
  }

  public static DataStream<String> rulesStreamToJson(DataStream<RulePojo> alerts) {
    return alerts.flatMap(new JsonSerializer<>(RulePojo.class)).name("Rules Deserialization");
  }

  public enum Type {
    KAFKA("Current Rules Sink (Kafka)"),
    PUBSUB("Current Rules Sink (Pub/Sub)"),
    STDOUT("Current Rules Sink (Std. Out)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
