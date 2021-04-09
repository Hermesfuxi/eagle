package bigdata.hermesfuxi.eagle.rules.service.router;

import bigdata.hermesfuxi.eagle.rules.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rules.pojo.ResultBean;
import bigdata.hermesfuxi.eagle.rules.service.RuleProcessFunctionV5;
import bigdata.hermesfuxi.eagle.rules.utils.KafkaSourceFunctions;
import bigdata.hermesfuxi.eagle.rules.utils.StateDescUtil;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hermesfuxi
 * desc 静态规则引擎版本2主程序
 * 相对于v4.0来说，只有一处改变： 新增了规则流的读取
 */
public class RuleEngineV5 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加一个消费kafka中用户实时行为事件数据的source
        DataStreamSource<String> logStream = env.addSource(KafkaSourceFunctions.getKafkaEventSource());

        // 将json格式的数据，转成 logBean格式的数据
        SingleOutputStreamOperator<LogBean> beanStream = logStream.map(new MapFunction<String, LogBean>() {
            @Override
            public LogBean map(String value) throws Exception {
                return JSON.parseObject(value,LogBean.class);
            }
        });

        // 对数据按用户deviceid分key
        // TODO 后续可以升级改造成 动态keyBy
        KeyedStream<LogBean, String> keyed = beanStream.keyBy(LogBean::getDeviceId);


        // 读取规则信息流
        DataStreamSource<String> ruleStream = env.addSource(KafkaSourceFunctions.getKafkaRuleSource());
        // 广播
        BroadcastStream<String> broadcastStream = ruleStream.broadcast(StateDescUtil.ruleKieStateDesc);

        // 连接  事件流 & 规则广播流
        BroadcastConnectedStream<LogBean, String> connected = keyed.connect(broadcastStream);

        // 开始核心计算处理
        SingleOutputStreamOperator<ResultBean> resultStream = connected.process(new RuleProcessFunctionV5());


        // 打印
        resultStream.print();

        env.execute();
    }
}
