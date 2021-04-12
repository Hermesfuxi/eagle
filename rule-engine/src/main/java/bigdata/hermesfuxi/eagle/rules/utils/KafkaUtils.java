package bigdata.hermesfuxi.eagle.rules.utils;

import bigdata.hermesfuxi.eagle.rules.config.Config;
import java.util.Properties;

import static bigdata.hermesfuxi.eagle.rules.config.Parameters.*;

public class KafkaUtils {

  public static Properties initConsumerProperties(Config config) {
    Properties kafkaProps = initProperties(config);
    String offset = config.get(OFFSET);
    kafkaProps.setProperty("auto.offset.reset", offset);
    return kafkaProps;
  }

  public static Properties initProducerProperties(Config params) {
    return initProperties(params);
  }

  private static Properties initProperties(Config config) {
    Properties kafkaProps = new Properties();
    String kafkaHost = config.get(KAFKA_HOST);
    int kafkaPort = config.get(KAFKA_PORT);
    String servers = String.format("%s:%s", kafkaHost, kafkaPort);
    kafkaProps.setProperty("bootstrap.servers", servers);
    return kafkaProps;
  }
}
