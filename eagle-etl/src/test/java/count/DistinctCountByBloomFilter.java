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
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
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

统计结果：(A1,view,2,4) (A1,join,2,3) (A2,view,1,2) (A2,join,1,1)
浏览人数：A1,view,2
浏览次数：A1,view,4
参与人数：A1,join,2
参与次数：A1,join,3
 */

/**
 * 活动事件表的用户统计（UV - count(distinct uid))与用户浏览统计（PV - count(udi)）：HashSet实现
 * 缺点：HashSet不能太大，很容易占用太多内存，不推荐使用
 */
public class DistinctCountByBloomFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //u1,A1,view
        DataStreamSource<String> lines = env.socketTextStream("hadoop-slave3", 8888);

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

            private transient ValueState<Integer> uidDistinctCountState;
            private transient ValueState<Integer> uidCountState;
            private transient ValueState<BloomFilter<String>> bloomFilter;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> uidDistinctCountDescriptor = new ValueStateDescriptor<>("uid-distinct-count-state", Integer.class);
                uidDistinctCountState = getRuntimeContext().getState(uidDistinctCountDescriptor);

                ValueStateDescriptor<Integer> uidCountDescriptor = new ValueStateDescriptor<>("uid-count-state", Integer.class);
                uidCountState = getRuntimeContext().getState(uidCountDescriptor);

                ValueStateDescriptor<BloomFilter<String>> bloomFilterDescriptor = new ValueStateDescriptor<>("uid-filter-state", TypeInformation.of(new TypeHint<BloomFilter<String>>(){}));
                bloomFilter = getRuntimeContext().getState(bloomFilterDescriptor);

            }

            @Override
            public void processElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
                String uid = value.f0;

                Integer uidCount = uidCountState.value();
                if(uidCount == null) {
                    uidCount = 0;
                }
                uidCount++;
                uidCountState.update(uidCount);

                Integer uidDistinctCount = uidDistinctCountState.value();
                if(uidDistinctCount == null) {
                    uidDistinctCount = 0;
                }

                BloomFilter<String> filter = bloomFilter.value();
                if(filter == null){
                    filter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 1000000);
                }

                if(!filter.mightContain(uid)){
                    filter.put(uid);
                    uidDistinctCount ++;
                }
                uidDistinctCountState.update(uidDistinctCount);
                bloomFilter.update(filter);

                //输出
                out.collect(Tuple4.of(value.f1, value.f2, uidDistinctCount, uidCount));
            }
        }).print();


        env.execute();
    }
}
