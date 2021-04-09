package bigdata.hermesfuxi.eagle.etl.jobs;

import bigdata.hermesfuxi.eagle.etl.bean.DataLogBean;
import bigdata.hermesfuxi.eagle.etl.utils.FlinkUtils;
import bigdata.hermesfuxi.eagle.etl.utils.MyKafkaDeserializationSchema;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

/**
 *  关联维表:
 *      统计的具体指标
 *      1.各个组播的收到打上的积分值
 *      2.打赏礼物的数量、受欢迎礼物 topN
 *      3.做多维度的指标统计（ClickHouse）
 */
public class LiveGiftPointStatistics {
    public static void main(String[] args) throws Exception {
        FlinkUtils.env.setParallelism(1);
        DataStream<Tuple2<String, String>> kafkaSourceV2 = FlinkUtils.getKafkaSourceV2(args, MyKafkaDeserializationSchema.class);

        SingleOutputStreamOperator<DataLogBean> beanStream = kafkaSourceV2.process(new ProcessFunction<Tuple2<String, String>, DataLogBean>() {
            @Override
            public void processElement(Tuple2<String, String> value, Context ctx, Collector<DataLogBean> out) throws Exception {
                try {
                    DataLogBean dataLogBean = JSON.parseObject(value.f1, DataLogBean.class);
                    if (dataLogBean != null) {
                        dataLogBean.setGid(value.f0);
                        Map map = dataLogBean.getProperties();
                        if ("liveReward".equals(dataLogBean.getEventId()) && map != null && map.get("anchor_id") != null && map.get("gift_id") != null) {
                            dataLogBean.setAnchorId(map.get("anchor_id").toString());
                            dataLogBean.setGiftId(Integer.parseInt(map.get("gift_id").toString()));
                            out.collect(dataLogBean);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        beanStream.print();

        DataStreamSource<Tuple4<Integer, String, Integer, Integer>> liveGiftMap = FlinkUtils.env.addSource(new MySQLSource());
        // 被广播的一般是小表或字典表
        MapStateDescriptor<Integer, Tuple2<String, Integer>> mapStateDescriptor = new MapStateDescriptor<>("broadcastState", TypeInformation.of(Integer.class), TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));
        BroadcastStream<Tuple4<Integer, String, Integer, Integer>> broadcast = liveGiftMap.broadcast(mapStateDescriptor);

        OutputTag<Tuple2<String, Integer>> anchorPointsTag = new OutputTag<Tuple2<String, Integer>>("anchorPoints") {
        };
        OutputTag<Tuple2<String, Integer>> giftCountTag = new OutputTag<Tuple2<String, Integer>>("giftCount") {
        };

        SingleOutputStreamOperator<DataLogBean> result = beanStream.connect(broadcast).process(new LiveGiftPointProcess(mapStateDescriptor, anchorPointsTag, giftCountTag, 3));
        result.print("result");
        // 输出 clickhouse 做多维度分析

        DataStream<Tuple2<String, Integer>> anchorPointsStream = result.getSideOutput(anchorPointsTag);
//        anchorPointsStream.print("anchorPoints");
        SingleOutputStreamOperator<List<Map.Entry<String, Integer>>> anchorPointsResult = anchorPointsStream.keyBy(t -> t.f0).sum(1).keyBy(t -> "key").process(new SumOrderProcess());
        anchorPointsResult.print("anchorPoints");
        // 输出到 redis中，并展示大盘

        DataStream<Tuple2<String, Integer>> giftCountStream = result.getSideOutput(giftCountTag);
//        giftCountStream.print("giftCount");
        giftCountStream.keyBy(t->t.f0).sum(1).keyBy(t->"key").process(new SumOrderProcess()).print("giftCount");
        // 输出到 redis中，并展示大盘

        FlinkUtils.env.execute();
    }

    public static class MySQLSource extends RichSourceFunction<Tuple4<Integer, String, Integer, Integer>> {
        private boolean flag = true;
        private Connection connection;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/realtime?characterEncoding=UTF-8&serverTimezone=GMT%2B8&useSSL=false", "root", "123456");
        }

        /**
         * run方法在restoreState方法之后执行
         */
        @Override
        public void run(SourceContext<Tuple4<Integer, String, Integer, Integer>> ctx) throws Exception {
            long updateTime = 0;
            while (flag) {
                String sql = "SELECT id, name, points, deleted FROM tb_live_gift where updateTime > ? " + (updateTime == 0? "and deleted = 0": "");
                //查询数据库 SELECT * FROM t_category WHERE last_update > 上一次查询时间
                PreparedStatement prepareStatement = connection.prepareStatement(sql);
                prepareStatement.setDate(1, new Date(updateTime));
                updateTime = System.currentTimeMillis();

                ResultSet resultSet = prepareStatement.executeQuery();
                while (resultSet.next()) {
                    Integer id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    Integer points = resultSet.getInt("points");
                    Integer deleted = resultSet.getInt("deleted");
                    //最大的数据，以后根据最大的时间作为查询条件
                    ctx.collect(Tuple4.of(id, name, points, deleted));
                }
                resultSet.close();
                prepareStatement.close();
                Thread.sleep(10000);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private static class LiveGiftPointProcess extends BroadcastProcessFunction<DataLogBean, Tuple4<Integer, String, Integer, Integer>, DataLogBean> {
        MapStateDescriptor<Integer, Tuple2<String, Integer>> mapStateDescriptor;
        private Integer topN;

        private OutputTag<Tuple2<String, Integer>> anchorPointsTag;
        private OutputTag<Tuple2<String, Integer>> giftCountTag;

        public LiveGiftPointProcess(MapStateDescriptor<Integer, Tuple2<String, Integer>> mapStateDescriptor, OutputTag<Tuple2<String, Integer>> anchorPointsTag, OutputTag<Tuple2<String, Integer>> giftCountTag, Integer topN) {
            this.mapStateDescriptor = mapStateDescriptor;
            this.anchorPointsTag = anchorPointsTag;
            this.giftCountTag = giftCountTag;
            this.topN = topN;
        }

        @Override
        public void processElement(DataLogBean bean, ReadOnlyContext ctx, Collector<DataLogBean> out) throws Exception {
            ReadOnlyBroadcastState<Integer, Tuple2<String, Integer>> broadcastMap = ctx.getBroadcastState(mapStateDescriptor);
            Integer giftId = bean.getGiftId();
            String anchorId = bean.getAnchorId();
            if (broadcastMap != null && broadcastMap.contains(giftId)) {

                Tuple2<String, Integer> stringIntegerTuple2 = broadcastMap.get(giftId);

                Integer points = stringIntegerTuple2.f1;
                bean.setPoints(points);

                String giftName = stringIntegerTuple2.f0;
                bean.setGiftName(giftName);

                out.collect(bean);
                ctx.output(anchorPointsTag, Tuple2.of(anchorId, points));
                ctx.output(giftCountTag, Tuple2.of(giftName, 1));
            }
        }

        @Override
        public void processBroadcastElement(Tuple4<Integer, String, Integer, Integer> value, Context ctx, Collector<DataLogBean> out) throws Exception {
            Integer id = value.f0;
            Integer deleted = value.f3;
            BroadcastState<Integer, Tuple2<String, Integer>> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            if(deleted == 1){
                broadcastState.remove(id);
            }else {
                broadcastState.put(id, Tuple2.of(value.f1, value.f2));
            }

        }
    }

    public static class SumOrderProcess extends KeyedProcessFunction<String, Tuple2<String, Integer>, List<Map.Entry<String, Integer>>> {
        private transient ValueState<Map<String, Integer>> mapValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("listValueState", TypeInformation.of(new TypeHint<Map<String, Integer>>() {
            })));
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<List<Map.Entry<String, Integer>>> out) throws Exception {
            Map<String, Integer> map = mapValueState.value();
            if(map == null){
                map = new HashMap<>();
            }
            map.put(value.f0, value.f1);
            mapValueState.update(map);

            long currentTimeMillis = System.currentTimeMillis();
            long triggerTime = currentTimeMillis - currentTimeMillis % 10000 + 10000;
            ctx.timerService().registerProcessingTimeTimer(triggerTime);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<Map.Entry<String, Integer>>> out) throws Exception {
            Map<String, Integer> map = mapValueState.value();
            List<Map.Entry<String, Integer>> list = map.entrySet().stream().sorted((a, b) -> b.getValue() - a.getValue()).collect(Collectors.toList());
            out.collect(list);
        }
    }
}