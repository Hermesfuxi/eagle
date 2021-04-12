package bigdata.hermesfuxi.eagle.rules.engine;

import static bigdata.hermesfuxi.eagle.rules.config.Parameters.CHECKPOINT_INTERVAL;
import static bigdata.hermesfuxi.eagle.rules.config.Parameters.ENABLE_CHECKPOINTS;
import static bigdata.hermesfuxi.eagle.rules.config.Parameters.LOCAL_EXECUTION;
import static bigdata.hermesfuxi.eagle.rules.config.Parameters.MIN_PAUSE_BETWEEN_CHECKPOINTS;
import static bigdata.hermesfuxi.eagle.rules.config.Parameters.OUT_OF_ORDERNESS;
import static bigdata.hermesfuxi.eagle.rules.config.Parameters.RULES_SOURCE;
import static bigdata.hermesfuxi.eagle.rules.config.Parameters.SOURCE_PARALLELISM;

import bigdata.hermesfuxi.eagle.rules.config.Config;
import bigdata.hermesfuxi.eagle.rules.config.Transaction;
import bigdata.hermesfuxi.eagle.rules.dynamicrules.functions.AverageAggregate;
import bigdata.hermesfuxi.eagle.rules.dynamicrules.functions.DynamicAlertFunction;
import bigdata.hermesfuxi.eagle.rules.dynamicrules.functions.DynamicKeyFunction;
import bigdata.hermesfuxi.eagle.rules.pojo.Alert;
import bigdata.hermesfuxi.eagle.rules.pojo.RulePojo;
import bigdata.hermesfuxi.eagle.rules.sinks.AlertsSink;
import bigdata.hermesfuxi.eagle.rules.sinks.CurrentRulesSink;
import bigdata.hermesfuxi.eagle.rules.sinks.LatencySink;
import bigdata.hermesfuxi.eagle.rules.source.RulesSource;
import bigdata.hermesfuxi.eagle.rules.source.TransactionsSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RulesEvaluator {

  private Config config;

  RulesEvaluator(Config config) {
    this.config = config;
  }

  public void run() throws Exception {

    RulesSource.Type rulesSourceType = getRulesSourceType();

    boolean isLocal = config.get(LOCAL_EXECUTION);
    boolean enableCheckpoints = config.get(ENABLE_CHECKPOINTS);
    int checkpointsInterval = config.get(CHECKPOINT_INTERVAL);
    int minPauseBtwnCheckpoints = config.get(CHECKPOINT_INTERVAL);

    // Environment setup
    StreamExecutionEnvironment env = configureStreamExecutionEnvironment(rulesSourceType, isLocal);

    if (enableCheckpoints) {
      env.enableCheckpointing(checkpointsInterval);
      env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBtwnCheckpoints);
    }

    // Streams setup
    DataStream<RulePojo> rulesUpdateStream = getRulesUpdateStream(env);
    DataStream<Transaction> transactions = getTransactionsStream(env);

    BroadcastStream<RulePojo> rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor);

    // Processing pipeline setup
    DataStream<Alert> alerts =
        transactions
            .connect(rulesStream)
            .process(new DynamicKeyFunction())
            .uid("DynamicKeyFunction")
            .name("Dynamic Partitioning Function")
            .keyBy((keyed) -> keyed.getKey())
            .connect(rulesStream)
            .process(new DynamicAlertFunction())
            .uid("DynamicAlertFunction")
            .name("Dynamic Rule Evaluation Function");

    DataStream<String> allRuleEvaluations =
        ((SingleOutputStreamOperator<Alert>) alerts).getSideOutput(Descriptors.demoSinkTag);

    DataStream<Long> latency =
        ((SingleOutputStreamOperator<Alert>) alerts).getSideOutput(Descriptors.latencySinkTag);

    DataStream<RulePojo> currentRules =
        ((SingleOutputStreamOperator<Alert>) alerts).getSideOutput(Descriptors.currentRulesSinkTag);

    alerts.print().name("Alert STDOUT Sink");
    allRuleEvaluations.print().setParallelism(1).name("Rule Evaluation Sink");

    DataStream<String> alertsJson = AlertsSink.alertsStreamToJson(alerts);
    DataStream<String> currentRulesJson = CurrentRulesSink.rulesStreamToJson(currentRules);

    currentRulesJson.print();

    alertsJson
        .addSink(AlertsSink.createAlertsSink(config))
        .setParallelism(1)
        .name("Alerts JSON Sink");
    currentRulesJson.addSink(CurrentRulesSink.createRulesSink(config)).setParallelism(1);

    DataStream<String> latencies =
        latency
            .timeWindowAll(Time.seconds(10))
            .aggregate(new AverageAggregate())
            .map(String::valueOf);
    latencies.addSink(LatencySink.createLatencySink(config));

    env.execute("Fraud Detection Engine");
  }

  private DataStream<Transaction> getTransactionsStream(StreamExecutionEnvironment env) {
    // Data stream setup
    SourceFunction<String> transactionSource = TransactionsSource.createTransactionsSource(config);
    int sourceParallelism = config.get(SOURCE_PARALLELISM);
    DataStream<String> transactionsStringsStream =
        env.addSource(transactionSource)
            .name("Transactions Source")
            .setParallelism(sourceParallelism);
    DataStream<Transaction> transactionsStream =
        TransactionsSource.stringsStreamToTransactions(transactionsStringsStream);
    return transactionsStream.assignTimestampsAndWatermarks(
        new SimpleBoundedOutOfOrdernessTimestampExtractor<>(config.get(OUT_OF_ORDERNESS)));
  }

  private DataStream<RulePojo> getRulesUpdateStream(StreamExecutionEnvironment env) throws IOException {

    RulesSource.Type rulesSourceEnumType = getRulesSourceType();

    SourceFunction<String> rulesSource = RulesSource.createRulesSource(config);
    DataStream<String> rulesStrings =
        env.addSource(rulesSource).name(rulesSourceEnumType.getName()).setParallelism(1);
    return RulesSource.stringsStreamToRules(rulesStrings);
  }

  private RulesSource.Type getRulesSourceType() {
    String rulesSource = config.get(RULES_SOURCE);
    return RulesSource.Type.valueOf(rulesSource.toUpperCase());
  }

  private StreamExecutionEnvironment configureStreamExecutionEnvironment(
      RulesSource.Type rulesSourceEnumType, boolean isLocal) {
    Configuration flinkConfig = new Configuration();
    flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

    StreamExecutionEnvironment env =
        isLocal
            ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
            : StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getCheckpointConfig().setCheckpointInterval(config.get(CHECKPOINT_INTERVAL));
    env.getCheckpointConfig()
        .setMinPauseBetweenCheckpoints(config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS));

    configureRestartStrategy(env, rulesSourceEnumType);
    return env;
  }

  private static class SimpleBoundedOutOfOrdernessTimestampExtractor<T extends Transaction>
      extends BoundedOutOfOrdernessTimestampExtractor<T> {

    public SimpleBoundedOutOfOrdernessTimestampExtractor(int outOfOrderdnessMillis) {
      super(Time.of(outOfOrderdnessMillis, TimeUnit.MILLISECONDS));
    }

    @Override
    public long extractTimestamp(T element) {
      return element.getEventTime();
    }
  }

  private void configureRestartStrategy(
      StreamExecutionEnvironment env, RulesSource.Type rulesSourceEnumType) {
    switch (rulesSourceEnumType) {
      case SOCKET:
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(
                10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
        break;
      case KAFKA:
        // Default - unlimited restart strategy.
        //        env.setRestartStrategy(RestartStrategies.noRestart());
    }
  }

  public static class Descriptors {
    public static final MapStateDescriptor<Integer, RulePojo> rulesDescriptor =
        new MapStateDescriptor<>(
            "rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(RulePojo.class));

    public static final OutputTag<String> demoSinkTag = new OutputTag<String>("demo-sink") {};
    public static final OutputTag<Long> latencySinkTag = new OutputTag<Long>("latency-sink") {};
    public static final OutputTag<RulePojo> currentRulesSinkTag =
        new OutputTag<RulePojo>("current-rules-sink") {};
  }
}
