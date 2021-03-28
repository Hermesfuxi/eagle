package bigdata.hermesfuxi.eagle.etl.utils;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.Arrays;
import java.util.Properties;

public class FlinkUtils {
    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<T> getKafkaSource(String[] args, Class<? extends DeserializationSchema<T>> deserializer) throws Exception {
        // 从配置文件中读取相关参数（输入参数配置文件名）
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        long checkpointInterval = parameterTool.getLong("flink.checkpoint.interval", 10000L);
        String stateBackEndPath = parameterTool.get("flink.stateBackEnd.path");

        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，checkpoint有多个，可以根据实际需要恢复到指定的Checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new RocksDBStateBackend(stateBackEndPath));

        // 使用反射生成反序列化实例
        DeserializationSchema<T> deserializationSchema = deserializer.newInstance();
        // JDK9之后，推荐使用（上面的显示已过时）
//        DeserializationSchema<T> deserializationSchema = deserializer.getDeclaredConstructor().newInstance();

        ParameterTool kafkaParameterTool = ParameterTool.fromPropertiesFile(args[1]);
        // 从设置kafka参数
        String[] topics = kafkaParameterTool.get("topics").split(",");
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<T>(Arrays.asList(topics), deserializationSchema, kafkaParameterTool.getProperties());
        // 在Checkpoint时不将偏移量写入到Kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);
    }

    /**
     *  用于读取Kafka消费数据中的topic、partition、offset
     */
    public static <T> DataStream<T> getKafkaSourceV2(String[] args, Class<? extends KafkaDeserializationSchema<T>> deserializer) throws Exception {
        // 从配置文件中读取相关参数（输入参数配置文件名）
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        long checkpointInterval = parameterTool.getLong("flink.checkpoint.interval", 30000L);
        String stateBackEndPath = parameterTool.get("flink.stateBackEnd.path");

        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，checkpoint有多个，可以根据实际需要恢复到指定的Checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new RocksDBStateBackend(stateBackEndPath));

        // 使用反射生成反序列化实例
        KafkaDeserializationSchema<T> deserializationSchema = deserializer.newInstance();

        // 从设置kafka参数
        ParameterTool kafkaParameterTool = ParameterTool.fromPropertiesFile(args[1]);
        String[] topics = kafkaParameterTool.get("topics").split(",");
        // 从设置kafka参数
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<T>(Arrays.asList(topics), deserializationSchema, kafkaParameterTool.getProperties());

        // 在Checkpoint时不将偏移量写入到Kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);
    }
}

