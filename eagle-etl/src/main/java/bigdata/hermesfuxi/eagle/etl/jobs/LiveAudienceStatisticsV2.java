package bigdata.hermesfuxi.eagle.etl.jobs;

import bigdata.hermesfuxi.eagle.etl.bean.DataLogBean;
import bigdata.hermesfuxi.eagle.etl.utils.FlinkUtils;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 直播观众统计V2：考虑时间间隔（在同一直播中，同一个观众，中间离开30分钟之内，算一个会话，否则算两个会话）
 */
public class LiveAudienceStatisticsV2 {
    public static void main(String[] args) throws Exception {
        DataStream<String> kafkaSource = FlinkUtils.getKafkaSource(args, SimpleStringSchema.class);

        SingleOutputStreamOperator<DataLogBean> beanStream = kafkaSource.process(new ProcessFunction<String, DataLogBean>() {
            @Override
            public void processElement(String value, Context ctx, Collector<DataLogBean> out) throws Exception {
                DataLogBean bean = JSON.parseObject(value, DataLogBean.class);
                if(bean != null && bean.getProperties() != null && bean.getProperties().containsKey("anchor_id") && bean.getEventId().startsWith("live")){
                    out.collect(bean);
                }
            }
        });

        KeyedStream<DataLogBean, Tuple2<String, String>> keyedStream = beanStream.keyBy(new KeySelector<DataLogBean, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(DataLogBean bean) throws Exception {
                return Tuple2.of(String.valueOf(bean.getProperties().get("anchor_id")), bean.getDeviceId());
            }
        });

        SingleOutputStreamOperator<Tuple5<String, String, Integer, Integer, Integer>> streamOperator = keyedStream.process(new LiveAudienceKeyedProcess());
        KeyedStream<Tuple5<String, String, Integer, Integer, Integer>, String> anchorIdKeyedStream = streamOperator.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple> distinctCountUv = anchorIdKeyedStream.sum(2).project(0, 2);
        distinctCountUv.print("distinctCountUv");

        SingleOutputStreamOperator<Tuple> realTimeUv = anchorIdKeyedStream.sum(3).project(0, 3);
        realTimeUv.print("realTimeUv");

        SingleOutputStreamOperator<Tuple> pvCount = anchorIdKeyedStream.sum(4).project(0, 4);
        pvCount.print("pvCount");

        FlinkUtils.env.execute();
    }

    private static class LiveAudienceKeyedProcess extends KeyedProcessFunction<Tuple2<String, String>, DataLogBean, Tuple5<String, String, Integer, Integer, Integer>> {
        private transient ValueState<Long> startTimeState;
        private transient ValueState<Long> endTimeState;
        private transient ValueState<Integer> onLineUserState;
        private transient ValueState<Integer> pvCountState;
        private transient ValueState<Integer> uvCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> startTimeStateDescriptor = new ValueStateDescriptor<>(
                    "startTimeStateDescriptor",
                    TypeInformation.of(Long.class)
            );
            startTimeState = getRuntimeContext().getState(startTimeStateDescriptor);

            ValueStateDescriptor<Long> endTimeStateDescriptor = new ValueStateDescriptor<>(
                    "endTimeStateDescriptor",
                    TypeInformation.of(Long.class)
            );
            endTimeState = getRuntimeContext().getState(endTimeStateDescriptor);

            ValueStateDescriptor<Integer> uvCountDescriptor = new ValueStateDescriptor<>(
                    "uvCountDescriptor",
                    TypeInformation.of(Integer.class)
            );
            uvCountState = getRuntimeContext().getState(uvCountDescriptor);

            ValueStateDescriptor<Integer> pvCountDescriptor = new ValueStateDescriptor<>(
                    "pvCountDescriptor",
                    TypeInformation.of(Integer.class)
            );
            pvCountState = getRuntimeContext().getState(pvCountDescriptor);

            ValueStateDescriptor<Integer> realTimeUvDescriptor = new ValueStateDescriptor<>(
                    "realTimeUvDescriptor",
                    TypeInformation.of(Integer.class)
            );
            onLineUserState = getRuntimeContext().getState(realTimeUvDescriptor);
        }

        @Override
        public void processElement(DataLogBean bean, Context ctx, Collector<Tuple5<String, String, Integer, Integer, Integer>> out) throws Exception {
            String eventId = bean.getEventId();
            // 当前时间
            Long timestamp = bean.getTimestamp();

            Integer uvCount = uvCountState.value();
            if (uvCount == null) {
                uvCount = 1;
            }else {
                uvCount = 0;
            }

            Integer pvCount = pvCountState.value();
            if (pvCount == null) {
                pvCount = 0;
            }

            Integer realTimeUv = onLineUserState.value();
            if (realTimeUv == null) {
                realTimeUv = 0;
            }

            Long startTime = startTimeState.value();
            Long endTime = endTimeState.value();

            //输入一条数据，先攒起来，不输出，先不考虑乱序问题
            // 拉链表： StartTime, endTime
            if ("liveEnter".equals(eventId)) {
                realTimeUv ++;
                // 第一次进入时，startTime为0
                if (startTime == null) {
                    pvCount++;
                    startTime = timestamp;
                } else {
                    // 只处理有结束的，其他如：有开始，没结束，代表持续至今，不做处理
                    if (endTime != null) {
                        if (startTime - endTime < 30 * 60 * 1000) {
                            // 合并， startTime不变，endTime归零
                            endTime = null;
                        } else {
                            // 不合并，startTime替换，endTime归零
                            pvCount++;
                            startTime = timestamp;
                            endTime = null;
                        }
                    }
                }
            } else if ("liveLeave".equals(eventId)) {
                realTimeUv --;
                endTime = timestamp;
            }
            startTimeState.update(startTime);
            endTimeState.update(endTime);
            pvCountState.update(pvCount);
            onLineUserState.update(realTimeUv);
            out.collect(Tuple5.of(String.valueOf(bean.getProperties().get("anchor_id")), bean.getDeviceId(), uvCount, realTimeUv, pvCount));
        }
    }
}
