package bigdata.hermesfuxi.eagle.rules.source;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import bigdata.hermesfuxi.eagle.rules.config.Config;
import bigdata.hermesfuxi.eagle.rules.dynamicrules.functions.RuleDeserializer;
import bigdata.hermesfuxi.eagle.rules.pojo.RulePojo;
import bigdata.hermesfuxi.eagle.rules.utils.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import static bigdata.hermesfuxi.eagle.rules.config.Parameters.*;

public class RulesSource {

  private static final int RULES_STREAM_PARALLELISM = 1;

  public static SourceFunction<String> createRulesSource(Config config) throws IOException {

    String sourceType = config.get(RULES_SOURCE);
    Type rulesSourceType = Type.valueOf(sourceType.toUpperCase());

    switch (rulesSourceType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
        String rulesTopic = config.get(RULES_TOPIC);
        FlinkKafkaConsumer<String> kafkaConsumer =
            new FlinkKafkaConsumer<>(rulesTopic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
      case SOCKET:
        return new SocketTextStreamFunction("localhost", config.get(SOCKET_PORT), "\n", -1);
      default:
        throw new IllegalArgumentException(
            "Source \"" + rulesSourceType + "\" unknown. Known values are:" + Type.values());
    }
  }

  public static DataStream<RulePojo> stringsStreamToRules(DataStream<String> ruleStrings) {
    return ruleStrings
        .flatMap(new RuleDeserializer())
        .name("Rule Deserialization")
        .setParallelism(RULES_STREAM_PARALLELISM)
        .assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<RulePojo>(Time.of(0, TimeUnit.MILLISECONDS)) {
              @Override
              public long extractTimestamp(RulePojo element) {
                // Prevents connected data+update stream watermark stalling.
                return Long.MAX_VALUE;
              }
            });
  }

  public enum Type {
    KAFKA("Rules Source (Kafka)"),
    PUBSUB("Rules Source (Pub/Sub)"),
    SOCKET("Rules Source (Socket)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
