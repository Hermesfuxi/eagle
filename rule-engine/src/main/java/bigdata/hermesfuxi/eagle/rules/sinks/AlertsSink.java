package bigdata.hermesfuxi.eagle.rules.sinks;

import bigdata.hermesfuxi.eagle.rules.config.Config;
import bigdata.hermesfuxi.eagle.rules.dynamicrules.functions.JsonSerializer;
import java.io.IOException;
import java.util.Properties;

import bigdata.hermesfuxi.eagle.rules.pojo.Alert;
import bigdata.hermesfuxi.eagle.rules.utils.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import static bigdata.hermesfuxi.eagle.rules.config.Parameters.*;

public class AlertsSink {

  public static SinkFunction<String> createAlertsSink(Config config) throws IOException {

    String sinkType = config.get(ALERTS_SINK);
    Type alertsSinkType = Type.valueOf(sinkType.toUpperCase());

    switch (alertsSinkType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initProducerProperties(config);
        String alertsTopic = config.get(ALERTS_TOPIC);
        return new FlinkKafkaProducer<>(alertsTopic, new SimpleStringSchema(), kafkaProps);
      case STDOUT:
        return new PrintSinkFunction<>(true);
      default:
        throw new IllegalArgumentException(
            "Source \"" + alertsSinkType + "\" unknown. Known values are:" + Type.values());
    }
  }

  public static DataStream<String> alertsStreamToJson(DataStream<Alert> alerts) {
    return alerts.flatMap(new JsonSerializer<>(Alert.class)).name("Alerts Deserialization");
  }

  public enum Type {
    KAFKA("Alerts Sink (Kafka)"),
    PUBSUB("Alerts Sink (Pub/Sub)"),
    STDOUT("Alerts Sink (Std. Out)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
