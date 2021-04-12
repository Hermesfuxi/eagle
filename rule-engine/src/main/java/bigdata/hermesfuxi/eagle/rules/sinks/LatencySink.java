package bigdata.hermesfuxi.eagle.rules.sinks;

import java.io.IOException;
import java.util.Properties;

import bigdata.hermesfuxi.eagle.rules.config.Config;
import bigdata.hermesfuxi.eagle.rules.utils.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import static bigdata.hermesfuxi.eagle.rules.config.Parameters.LATENCY_SINK;
import static bigdata.hermesfuxi.eagle.rules.config.Parameters.LATENCY_TOPIC;

public class LatencySink {

  public static SinkFunction<String> createLatencySink(Config config) throws IOException {

    String latencySink = config.get(LATENCY_SINK);
    Type latencySinkType = Type.valueOf(latencySink.toUpperCase());

    switch (latencySinkType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initProducerProperties(config);
        String latencyTopic = config.get(LATENCY_TOPIC);
        return new FlinkKafkaProducer<>(latencyTopic, new SimpleStringSchema(), kafkaProps);
      case STDOUT:
        return new PrintSinkFunction<>(true);
      default:
        throw new IllegalArgumentException(
            "Source \"" + latencySinkType + "\" unknown. Known values are:" + Type.values());
    }
  }

  public enum Type {
    KAFKA("Latency Sink (Kafka)"),
    PUBSUB("Latency Sink (Pub/Sub)"),
    STDOUT("Latency Sink (Std. Out)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
