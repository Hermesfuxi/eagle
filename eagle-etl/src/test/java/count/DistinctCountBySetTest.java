package count;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/*
数据：分别对应（用户ID-userId,活动ID-activeId,事件ID-eventId）
u1,A1,view
u1,A1,view
u1,A1,view
u1,A1,join
u1,A1,join
u2,A1,view
u2,A1,join
u1,A2,view
u1,A2,view
u1,A2,join

统计结果：
浏览次数：A1,view,4
浏览人数：A1,view,2
参与次数：A1,join,3
参与人数：A1,join,2
 */

/**
 * 活动事件表的用户统计（UV - count(distinct uid))与用户浏览统计（PV - count(udi)）：HashSet实现
 * 缺点：HashSet不能太大，很容易占用太多内存，不推荐使用
 */
public class DistinctCountBySetTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //u1,A1,view
        DataStreamSource<String> lines = env.socketTextStream("hadoop-slaved3", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        //按照活动ID，事件ID进行keyBy，同一个活动、同一种事件的用户一定会进入到同一个分区
        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStream = tpStream.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return Tuple2.of(value.f1, value.f2);
            }
        });

        //在同一组内进行聚合
        keyedStream.process(new KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>>() {

            private transient ValueState<HashSet<String>> uidState;
            private transient ValueState<Integer> countState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<HashSet<String>> stateDescriptor = new ValueStateDescriptor<>("uids-state", TypeInformation.of(new TypeHint<HashSet<String>>(){}));
                uidState = getRuntimeContext().getState(stateDescriptor);

                ValueStateDescriptor<Integer> countStateDescriptor = new ValueStateDescriptor<>("uid-count-state", Integer.class);
                countState = getRuntimeContext().getState(countStateDescriptor);

            }

            @Override
            public void processElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
                String uid = value.f0;

                HashSet<String> set = uidState.value();
                if(set == null) {
                    set = new HashSet<>();
                }
                set.add(uid);
                //更新状态
                uidState.update(set);

                Integer count = countState.value();
                if(count == null) {
                    count = 0;
                }
                count++;
                //更新状态
                countState.update(count);

                //输出
                out.collect(Tuple4.of(value.f1, value.f2, set.size(), count));
            }
        }).print();


        env.execute();
    }
}
