package bigdata.hermesfuxi.eagle.rules.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSourceFunctions {
    public static FlinkKafkaConsumer<String> getKafkaEventSource(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092\n");
        props.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<>("eagle_app_log", new SimpleStringSchema(), props);
        return source;
    }


    public static FlinkKafkaConsumer<String> getKafkaRuleSource() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092");
        props.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<>("eagle_drl_rule", new SimpleStringSchema(), props);
        return source;
    }
}
