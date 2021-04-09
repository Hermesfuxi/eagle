package bigdata.hermesfuxi.eagle.rules.engine;

import bigdata.hermesfuxi.eagle.rules.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rules.pojo.ResultBean;
import bigdata.hermesfuxi.eagle.rules.pojo.RuleParam;
import bigdata.hermesfuxi.eagle.rules.service.*;
import bigdata.hermesfuxi.eagle.rules.service.realtime.RealTimeRuleQueryCalculateService;
import bigdata.hermesfuxi.eagle.rules.service.realtime.UserActionCountQueryServiceStateImpl;
import bigdata.hermesfuxi.eagle.rules.service.realtime.UserActionSequenceQueryServiceStateImpl;
import bigdata.hermesfuxi.eagle.rules.utils.RuleSimulator;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author hermesfuxi
 * desc 实时运营系统版本1.0
 * <p>
 * 需求中要实现的判断规则：
 * 触发条件：E事件
 * 画像属性条件：  k3=v3 , k100=v80 , k230=v360
 * 行为属性条件：  U(p1=v3,p2=v2) >= 3次 且  G(p6=v8,p4=v5,p1=v2)>=1
 * 行为次序条件：  依次做过：  W(p1=v4) ->   R(p2=v3) -> F
 */
public class RuleEngineV1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，checkpoint有多个，可以根据实际需要恢复到指定的Checkpoint
//        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setStateBackend(new RocksDBStateBackend("file:///D://WorkSpaces//IdeaProjects//eagle/.ck/flink"));

        // 创建一个kafka数据源source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("group.id", "group002");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("eagle-app-log", new SimpleStringSchema(), props);

        // 将数据源添加到env中
        DataStreamSource<String> logStream = env.addSource(kafkaSource);

        // 将json串数据，转成bean对象数据
        SingleOutputStreamOperator<LogBean> beanStream = logStream.process(new ProcessFunction<String, LogBean>() {
            @Override
            public void processElement(String value, Context ctx, Collector<LogBean> out) throws Exception {
                try {
                    LogBean logBean = JSON.parseObject(value, LogBean.class);
                    if (logBean != null) {
                        out.collect(logBean);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // 在这个keyby后的数据流上，做规则判断
        SingleOutputStreamOperator<ResultBean> resultStream = beanStream.keyBy(LogBean::getDeviceId).process(new RuleProcessFunction());
        resultStream.writeAsText("data/out/flink");
        env.execute();
    }

    static class RuleProcessFunction extends KeyedProcessFunction<String, LogBean, ResultBean> {
        private transient ListState<LogBean> eventState;
        private transient UserProfileQueryService userProfileQueryService;
        private transient RealTimeRuleQueryCalculateService userActionCountQueryService;
        private transient RealTimeRuleQueryCalculateService userActionSequenceQueryService;
        private transient RuleParam ruleParam;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 定义一个list结构的state
            ListStateDescriptor<LogBean> eventStateDesc = new ListStateDescriptor<>("events_state", LogBean.class);
            eventState = getRuntimeContext().getListState(eventStateDesc);

            userProfileQueryService = new UserProfileQueryServiceHbaseImpl();
            userActionCountQueryService = new UserActionCountQueryServiceStateImpl();
            userActionSequenceQueryService = new UserActionSequenceQueryServiceStateImpl();
            ruleParam = RuleSimulator.getRuleParam();
        }

        @Override
        public void processElement(LogBean logBean, Context ctx, Collector<ResultBean> out) throws Exception {
            // 先将拿到的这条数据，存入state中攒起来
            eventState.add(logBean);
            Iterable<LogBean> logBeans = eventState.get();

            // 判断当前的用户行为是否满足规则中的触发条件
            // 触发条件
            if (ruleParam.getTriggerParam().getEventId().equals(logBean.getEventId())) {
                String deviceId = logBean.getDeviceId();

                boolean userProfileQueryFlag = userProfileQueryService.judgeProfileCondition(deviceId, ruleParam);

                // 如果画像属性条件全部满足
                if (userProfileQueryFlag) {
                    boolean userActionCountQueryFlag = userActionCountQueryService.ruleQueryCalculate(logBeans, ruleParam);
                    // 如果行为次数条件也满足
                    if (userActionCountQueryFlag) {
                        boolean userActionSequenceQuery = userActionSequenceQueryService.ruleQueryCalculate(logBeans, ruleParam);
                        // 若 用户行为次序列条件 也满足
                        if (userActionSequenceQuery) {
                            ResultBean resultBean = new ResultBean();
                            resultBean.setDeviceId(deviceId);
                            resultBean.setRuleId(ruleParam.getRuleId());
                            resultBean.setTimeStamp(logBean.getTimeStamp());
                            out.collect(resultBean);
                        }
                    }
                }
            }
        }
    }
}
