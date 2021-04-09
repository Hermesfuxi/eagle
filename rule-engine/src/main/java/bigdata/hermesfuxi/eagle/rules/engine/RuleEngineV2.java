package bigdata.hermesfuxi.eagle.rules.engine;

import bigdata.hermesfuxi.eagle.rules.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rules.pojo.ResultBean;
import bigdata.hermesfuxi.eagle.rules.pojo.AtomicRuleParam;
import bigdata.hermesfuxi.eagle.rules.pojo.RuleParam;
import bigdata.hermesfuxi.eagle.rules.service.UserProfileQueryService;
import bigdata.hermesfuxi.eagle.rules.service.UserProfileQueryServiceHbaseImpl;
import bigdata.hermesfuxi.eagle.rules.service.offline.OfflineRuleQueryCalculateService;
import bigdata.hermesfuxi.eagle.rules.service.offline.UserActionCountQueryServiceClickhouseImpl;
import bigdata.hermesfuxi.eagle.rules.service.offline.UserActionSequenceQueryServiceClickhouseImpl;
import bigdata.hermesfuxi.eagle.rules.service.realtime.RealTimeRuleQueryCalculateService;
import bigdata.hermesfuxi.eagle.rules.service.realtime.UserActionCountQueryServiceStateImpl;
import bigdata.hermesfuxi.eagle.rules.service.realtime.UserActionSequenceQueryServiceStateImpl;
import bigdata.hermesfuxi.eagle.rules.utils.RuleSimulator;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateUtils;
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

import java.util.*;

/**
 * @author hermesfuxi
 * desc 实时运营系统版本2.0
 *
 * 需求中要实现的判断规则：
 * 触发条件：E事件
 * 画像属性条件：  k3=v3 , k100=v80 , k230=v360
 * 行为属性条件：  U(p1=v3,p2=v2) >= 3次 且  G(p6=v8,p4=v5,p1=v2)>=1
 * 行为次序条件：  依次做过：  W(p1=v4) ->   R(p2=v3) -> F
 */
public class RuleEngineV2 {
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
        private transient OfflineRuleQueryCalculateService userActionCountQueryClickhouseService;
        private transient OfflineRuleQueryCalculateService userActionSequenceQueryClickhouseService;
        private transient RuleParam ruleParam;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 定义一个list结构的state
            ListStateDescriptor<LogBean> eventStateDesc = new ListStateDescriptor<>("events_state", LogBean.class);
            eventState = getRuntimeContext().getListState(eventStateDesc);

            userProfileQueryService = new UserProfileQueryServiceHbaseImpl();

            // 实时查询
            userActionCountQueryService = new UserActionCountQueryServiceStateImpl();
            userActionSequenceQueryService = new UserActionSequenceQueryServiceStateImpl();

            // 离线查询（通过clickhouse）
            userActionCountQueryClickhouseService = new UserActionCountQueryServiceClickhouseImpl();
            userActionSequenceQueryClickhouseService = new UserActionSequenceQueryServiceClickhouseImpl();

            ruleParam = RuleSimulator.getRuleParam();
        }

        @Override
        public void processElement(LogBean logBean, Context ctx, Collector<ResultBean> out) throws Exception {
            // 先将拿到的这条数据，存入state中攒起来
            eventState.add(logBean);
            Iterable<LogBean> logBeans = eventState.get();

            // 计算事件时间的前1小时的整点时间戳，作数据切割
            long splitPoint = DateUtils.addHours(DateUtils.ceiling(new Date(logBean.getTimeStamp()), Calendar.HOUR), -2).getTime();

            // 判断当前的用户行为是否满足规则中的触发条件
            // 触发条件
            if (ruleParam.getTriggerParam().getEventId().equals(logBean.getEventId())) {
                String deviceId = logBean.getDeviceId();
                boolean userProfileQueryFlag = userProfileQueryService.judgeProfileCondition(deviceId, ruleParam);
                // 如果画像属性条件全部满足
                if (userProfileQueryFlag) {

                    // --------- 判断次数条件 ------------
                    ArrayList<AtomicRuleParam> offlineRangeParams = new ArrayList<>();  // 离线条件list
                    ArrayList<AtomicRuleParam> realTimeRangeParams = new ArrayList<>();  // 实时条件list
                    List<AtomicRuleParam> userActionCountParams = ruleParam.getUserActionCountParams();
                    for (AtomicRuleParam userActionCountParam : userActionCountParams) {
                        if (userActionCountParam.getRangeStart() < splitPoint) {
                            // 如果条件起始时间 < 分界点，放入离线条件租
                            offlineRangeParams.add(userActionCountParam);
                        } else {
                            // 如果条件起始时间 >= 分界点，放入实时条件组
                            realTimeRangeParams.add(userActionCountParam);
                        }
                    }

                    // 在clickhouse中查询离线条件组
                    if (offlineRangeParams.size() > 0) {
                        // 将规则总参数对象中的“次数类条件”覆盖成： 远期条件组
                        ruleParam.setUserActionCountParams(offlineRangeParams);
                        boolean offlineUserActionCountQueryFlag = userActionCountQueryClickhouseService.ruleQueryCalculate(deviceId, ruleParam);
                        if (!offlineUserActionCountQueryFlag) {
                            return;
                        }
                    }

                    // 在state中计算实时条件组
                    if (realTimeRangeParams.size() > 0) {
                        // 将规则总参数对象中的“次数类条件”覆盖成： 近期条件组
                        ruleParam.setUserActionCountParams(realTimeRangeParams);
                        // 交给stateService对这一组条件进行计算
                        boolean realTimeUserActionCountQueryFlag = userActionCountQueryService.ruleQueryCalculate(logBeans, ruleParam);
                        if (!realTimeUserActionCountQueryFlag) {
                            return;
                        }
                    }

                    // UserActionCount 行为次数混合条件

                    // --------- 判断次序条件 ------------
                    List<AtomicRuleParam> userActionSequenceParams = ruleParam.getUserActionSequenceParams();

                    if (userActionSequenceParams != null && userActionSequenceParams.size() > 0) {
                        long rangeStart = userActionSequenceParams.get(0).getRangeStart();
                        if (rangeStart >= splitPoint) {
                            // flink 实时计算
                            if (!userActionSequenceQueryService.ruleQueryCalculate(logBeans, ruleParam)) {
                                return;
                            }
                        } else {
                            // clickhouse 计算
                            if (!userActionSequenceQueryClickhouseService.ruleQueryCalculate(deviceId, ruleParam)) {
                                return;
                            }
                        }

                    }

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
