package bigdata.hermesfuxi.eagle.etl.jobs;

import bigdata.hermesfuxi.eagle.etl.bean.DataLogBean;
import bigdata.hermesfuxi.eagle.etl.bean.ItemEventCount;
import bigdata.hermesfuxi.eagle.etl.utils.FlinkUtils;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class HotItemsWindowTopN {
    public static void main(String[] args) throws Exception {
        DataStream<String> kafkaSource = FlinkUtils.getKafkaSource(args, SimpleStringSchema.class);
        SingleOutputStreamOperator<DataLogBean> beanStream = kafkaSource.process(new ProcessFunction<String, DataLogBean>() {
            @Override
            public void processElement(String value, Context ctx, Collector<DataLogBean> out) throws Exception {
                DataLogBean bean = JSON.parseObject(value, DataLogBean.class);
                if (bean != null && bean.getEventId().startsWith("product")) {
                    Map map = bean.getProperties();
                    if (map != null && map.containsKey("category_id") && map.containsKey("product_id")) {
                        out.collect(bean);
                    }
                }
            }
        });

        //按照EventTime划分窗口
        SingleOutputStreamOperator<DataLogBean> beanStreamWithWaterMark = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DataLogBean>forBoundedOutOfOrderness(Duration.ofMillis(5000))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DataLogBean>() {
                            @Override
                            public long extractTimestamp(DataLogBean element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));

        // 使用<eventId、categoryId、productId> 分组
        KeyedStream<DataLogBean, Tuple3<String, String, String>> keyedStream = beanStreamWithWaterMark.keyBy(new KeySelector<DataLogBean, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(DataLogBean bean) throws Exception {
                String eventId = bean.getEventId();
                Map map = bean.getProperties();
                String categoryId = String.valueOf(map.get("category_id"));
                String productId = String.valueOf(map.get("product_id"));
                return Tuple3.of(eventId, categoryId, productId);
            }
        });

        WindowedStream<DataLogBean, Tuple3<String, String, String>, TimeWindow> window = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5)));

        SingleOutputStreamOperator<ItemEventCount> aggregateWindow = window.aggregate(new AggregateWindowFunction(), new DataCollectWindowFunction());

        KeyedStream<ItemEventCount, Tuple4<String, String, Long, Long>> tuple2KeyedStream = aggregateWindow.keyBy(new KeySelector<ItemEventCount, Tuple4<String, String, Long, Long>>() {
            @Override
            public Tuple4<String, String, Long, Long> getKey(ItemEventCount value) throws Exception {
                return Tuple4.of(value.getEventId(), value.getCategoryId(), value.getStart(), value.getEnd());
            }
        });

        SingleOutputStreamOperator<ItemEventCount> result = tuple2KeyedStream.process(new HotItemsWindowsTopNFunction(3));

        result.print();

        FlinkUtils.env.execute();
    }

    /**
     * window增量聚合计算（来一条，处理一条）
     */
    static class  AggregateWindowFunction implements AggregateFunction<DataLogBean, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(DataLogBean bean, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }

    }

    static class  DataCollectWindowFunction implements WindowFunction<Long, ItemEventCount, Tuple3<String, String, String>, TimeWindow> {
        @Override
        public void apply(Tuple3<String, String, String> key, TimeWindow window, Iterable<Long> input, Collector<ItemEventCount> out) throws Exception {
            Long count = input.iterator().next();
            long start = window.getStart();
            long end = window.getEnd();
            out.collect(new ItemEventCount(key.f0, key.f1, key.f2, count, start, end));
        }
    }


    /**
     * 窗口分组TopN函数
     */
    static class HotItemsWindowsTopNFunction extends KeyedProcessFunction<Tuple4<String, String, Long, Long>, ItemEventCount, ItemEventCount> {
        Integer topN;
        public HotItemsWindowsTopNFunction(Integer topN) {
            this.topN = topN;
        }

        public HotItemsWindowsTopNFunction() {
        }

        private transient ListState<ItemEventCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<>("aggregateWindowListState", TypeInformation.of(ItemEventCount.class)));
        }

        @Override
        public void processElement(ItemEventCount value, Context ctx, Collector<ItemEventCount> out) throws Exception {
            listState.add(value);

            ctx.timerService().registerEventTimeTimer(value.getEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ItemEventCount> out) throws Exception {
            Iterable<ItemEventCount> itemEventCounts = listState.get();
            listState.clear();

            List<ItemEventCount> list = CollectionUtil.iterableToList(itemEventCounts);
            list.sort((a, b) -> b.getCount().compareTo(a.getCount()));

            List<ItemEventCount> resultList = list.subList(0, Math.min(topN, list.size()));
            for (ItemEventCount itemEventCount : resultList) {
                out.collect(itemEventCount);
            }
        }
    }
}