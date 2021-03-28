package bigdata.hermesfuxi.eagle.rule.engine;

import bigdata.hermesfuxi.eagle.rule.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rule.pojo.ResultBean;
import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.CollectionUtils;
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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * @author hermesfuxi
 * desc 实时运营系统版本1.0
 *
 * 需求中要实现的判断规则：
 * 触发条件：E事件
 * 画像属性条件：  k3=v3 , k100=v80 , k230=v360
 * 行为属性条件：  U(p1=v3,p2=v2) >= 3次 且  G(p6=v8,p4=v5,p1=v2)>=1
 * 行为次序条件：  依次做过：  W(p1=v4) ->   R(p2=v3) -> F
 */
public class RuleEngineV0 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建一个kafka数据源source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092");
        props.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("eagle-app-log", new SimpleStringSchema(), props);

        // 将数据源添加到env中
        DataStreamSource<String> logStream = env.addSource(kafkaSource);

        // 将json串数据，转成bean对象数据
        SingleOutputStreamOperator<LogBean> beanStream = logStream.process(new ProcessFunction<String, LogBean>() {
            @Override
            public void processElement(String value, Context ctx, Collector<LogBean> out) throws Exception {
                try {
                    LogBean logBean = JSON.parseObject(value, LogBean.class);
                    if(logBean != null){
                        out.collect(logBean);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });

        // 在这个keyby后的数据流上，做规则判断
        SingleOutputStreamOperator<ResultBean> resultStream = beanStream.keyBy(LogBean::getDeviceId).process(new KeyedProcessFunction<String, LogBean, ResultBean>() {

            Connection conn;
            Table table;
            ListState<LogBean> eventState;


            @Override
            public void open(Configuration parameters) throws Exception {

                org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                conf.set("hbase.zookeeper.quorum", "hadoop-master:2181,hadoop-master2:2181,hadoop-slave1:2181,hadoop-slave2:2181,hadoop-slave3:2181");

                conn = ConnectionFactory.createConnection(conf);
                table = conn.getTable(TableName.valueOf("eagle.user_profile"));

                // 定义一个list结构的state
                ListStateDescriptor<LogBean> eventStateDesc = new ListStateDescriptor<>("events_state", LogBean.class);
                eventState = getRuntimeContext().getListState(eventStateDesc);
            }

            @Override
            public void processElement(LogBean logBean, Context ctx, Collector<ResultBean> out) throws Exception {

                // 先将拿到的这条数据，存入state中攒起来
                eventState.add(logBean);

                // 判断当前的用户行为是否满足规则中的触发条件
                // 触发条件：E事件
                if ("E".equals(logBean.getEventId())) {
                    // 判断画像属性条件：  k3=v3 , k100=v80 , k230=v360
                    // 查询hbase即可

                    // 构造查询条件
                    Get get = new Get(Bytes.toBytes(logBean.getDeviceId()));
                    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("k3"));
                    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("k100"));
                    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("k230"));

                    // 传入查询条件并查询
                    Result result = table.get(get);
                    String k3Value = new String(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("k3")));
                    String k100Value = new String(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("k100")));
                    String k230Value = new String(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("k230")));

                    // 如果画像属性条件全部满足
                    if ("v3".equals(k3Value) && "v80".equals(k100Value) && "v360".equals(k230Value)) {

                        // 则继续判断行为次数条件：  U(p1=v3,p2=v2) >= 3次 且  G(p6=v8,p4=v5,p1=v2)>=1

                        // 从state中捞出这个人历史以来的所有行为事件
                        Iterable<LogBean> logBeans = eventState.get();

                        int uCnt = 0;
                        int gCnt = 0;
                        for (LogBean bean : logBeans) {
                            // 计算U事件原子条件的次数
                            if ("U".equals(bean.getEventId())) {
                                Map<String, String> properties = bean.getProperties();
                                String p1Value = properties.get("p1");
                                String p2Value = properties.get("p2");
                                if ("v3".equals(p1Value) && "v2".equals(p2Value)) {
                                    uCnt++;
                                };
                            }

                            // 计算G事件原子条件的次数
                            if ("G".equals(bean.getEventId())) {
                                Map<String, String> properties = bean.getProperties();
                                String p6 = properties.get("p6");
                                String p4 = properties.get("p4");
                                String p1 = properties.get("p1");
                                if ("v8".equals(p6) && "v5".equals(p4) && "v2".equals(p1)) {
                                    gCnt++;
                                };
                            }

                        }

                        // 如果行为次数条件也满足
                        if (uCnt >= 3 && gCnt >= 1) {
                            ArrayList<LogBean> beanList = new ArrayList<>();
                            CollectionUtils.addAll(beanList, logBeans.iterator());
                            // 则，继续判断行为次序条件：  依次做过：  W(p1=v4) ->   R(p2=v3) -> F

                            int index = -1;
                            for (int i = 0; i < beanList.size(); i++) {
                                LogBean beani = beanList.get(i);
                                if ("W".equals(beani.getEventId())) {
                                    Map<String, String> properties = beani.getProperties();
                                    String p1 = properties.get("p1");
                                    if ("v4".equals(p1)) {
                                        index = i;
                                        break;
                                    }
                                }
                            }

                            int index2 = -1;
                            if (index >= 0 && index + 1 < beanList.size()) {

                                for (int i = index + 1; i < beanList.size(); i++) {
                                    LogBean beani = beanList.get(i);
                                    if ("R".equals(beani.getEventId())) {
                                        Map<String, String> properties = beani.getProperties();
                                        String p2 = properties.get("p2");
                                        if ("v3".equals(p2)) {
                                            index2 = i;
                                            break;
                                        }
                                    }
                                }
                            }

                            int index3 = -1;
                            if (index2 >= 0 && index2 + 1 < beanList.size()) {

                                for (int i = index2 + 1; i < beanList.size(); i++) {
                                    LogBean beani = beanList.get(i);
                                    if ("F".equals(beani.getEventId())) {
                                        index3 = i;
                                        break;
                                    }
                                }
                            }

                            if(index3>-1){
                                ResultBean resultBean = new ResultBean();
                                resultBean.setDeviceId(logBean.getDeviceId());
                                resultBean.setRuleId("test_rule_1");
                                resultBean.setTimeStamp(logBean.getTimeStamp());
                                out.collect(resultBean);
                            }
                        }
                    }
                }
            }
        });
        resultStream.print();
        env.execute();

    }
}
