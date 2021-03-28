package bigdata.hermesfuxi.eagle.etl.jobs;

import bigdata.hermesfuxi.eagle.etl.bean.DataLogBean;
import bigdata.hermesfuxi.eagle.etl.functions.JsonToBeanFunc;
import bigdata.hermesfuxi.eagle.etl.utils.FlinkUtils;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.UUID;

/**
 * 直播观众统计V1：最常见的统计 在线观众数、累计观众数、累计浏览数
 */
public class LiveAudienceStatisticsV1 {
    public static void main(String[] args) throws Exception {
        DataStream<String> kafkaSource = FlinkUtils.getKafkaSource(args, SimpleStringSchema.class);

        SingleOutputStreamOperator<DataLogBean> beanStream = kafkaSource.process(new ProcessFunction<String, DataLogBean>() {
            @Override
            public void processElement(String value, Context ctx, Collector<DataLogBean> out) throws Exception {
                DataLogBean bean = JSON.parseObject(value, DataLogBean.class);
                if (bean != null) {
                    out.collect(bean);
                }
            }
        });

        //按照进入直播间的事件ID进行过滤
        SingleOutputStreamOperator<DataLogBean> liveStream = beanStream.filter(new FilterFunction<DataLogBean>() {
            @Override
            public boolean filter(DataLogBean bean) throws Exception {
                String eventId = bean.getEventId();
                Map properties = bean.getProperties();
                return properties != null && properties.get("anchor_id") != null && ("liveEnter".equals(eventId) || "liveLeave".equals(eventId));
            }
        });

        KeyedStream<DataLogBean, String> keyedStream = liveStream.keyBy(bean -> bean.getProperties().get("anchor_id").toString());

        SingleOutputStreamOperator<Tuple4<String, Integer, Integer, Integer>> result = keyedStream.process(new KeyedProcessFunction<String, DataLogBean, Tuple4<String, Integer, Integer, Integer>>() {
            private transient ValueState<Integer> uvState;
            private transient ValueState<Integer> pvState;
            private transient ValueState<BloomFilter<String>> bloomFilterState;
            private transient ValueState<Integer> onLineUserState;

            @Override
            public void open(Configuration parameters) throws Exception {
                uvState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("uvState", Integer.class));
                pvState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("pvState", Integer.class));
                onLineUserState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("onLineUserState", Integer.class));
                bloomFilterState = getRuntimeContext().getState(new ValueStateDescriptor<BloomFilter<String>>("bloomFilterState", TypeInformation.of(new TypeHint<BloomFilter<String>>() {
                })));
            }

            @Override
            public void processElement(DataLogBean bean, Context ctx, Collector<Tuple4<String, Integer, Integer, Integer>> out) throws Exception {
                Integer pvCount = pvState.value();
                if (pvCount == null) {
                    pvCount = 0;
                }

                Integer onLineUserCount = onLineUserState.value();
                if (onLineUserCount == null) {
                    onLineUserCount = 0;
                }

                BloomFilter<String> bloomFilter = bloomFilterState.value();
                if (bloomFilter == null) {
                    bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000000);
                }
                Integer uvCount = uvState.value();
                if (uvCount == null) {
                    uvCount = 0;
                }

                String anchorId = bean.getProperties().get("anchor_id").toString();
                String eventId = bean.getEventId();
                String deviceId = bean.getDeviceId();

                if ("liveEnter".equals(eventId)) {
                    pvCount++;
                    pvState.update(pvCount);

                    onLineUserCount++;
                    if(!bloomFilter.mightContain(deviceId)){
                        uvCount++;
                        uvState.update(uvCount);

                        bloomFilter.put(deviceId);
                        bloomFilterState.update(bloomFilter);
                    }
                }else if ("liveLeave".equals(eventId)) {
                    onLineUserCount--;
                }
                onLineUserState.update(onLineUserCount);
                out.collect(Tuple4.of(anchorId, onLineUserCount, uvCount, pvCount));
            }
        });

        result.print();

        FlinkUtils.env.execute();
    }
}
