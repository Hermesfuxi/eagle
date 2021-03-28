package bigdata.hermesfuxi.eagle.etl.jobs;

import bigdata.hermesfuxi.eagle.etl.bean.DataLogBean;
import bigdata.hermesfuxi.eagle.etl.functions.LocationAsyncAMapFunction;
import bigdata.hermesfuxi.eagle.etl.functions.MySQLTwoPhaseCommitSink;
import bigdata.hermesfuxi.eagle.etl.utils.FlinkUtils;
import bigdata.hermesfuxi.eagle.etl.utils.MyKafkaDeserializationSchema;
import ch.hsr.geohash.GeoHash;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

import static bigdata.hermesfuxi.eagle.etl.constant.Constants.GEO_HASH_LENGTH;

/**
 * @author Hermesfuxi
 */
public class PreETL {
    public static void main(String[] args) throws Exception {
        FlinkUtils.env.setParallelism(4);
        DataStream<String> kafkaSource = FlinkUtils.getKafkaSource(args, SimpleStringSchema.class);

        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };

        SingleOutputStreamOperator<DataLogBean> beanStream = kafkaSource.process(new ProcessFunction<String, DataLogBean>() {
            @Override
            public void processElement(String value, Context ctx, Collector<DataLogBean> out) throws Exception {
                try {
                    DataLogBean dataLogBean = JSON.parseObject(value, DataLogBean.class);
                    if (dataLogBean != null) {
                        if (dataLogBean.getLatitude() != null && dataLogBean.getLongitude() != null) {
                            Double latitude = dataLogBean.getLatitude();
                            Double longitude = dataLogBean.getLongitude();
                            String geoHashCode = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, GEO_HASH_LENGTH);
                            dataLogBean.setGeoHashCode(geoHashCode);
                        }
                        out.collect(dataLogBean);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    ctx.output(errorTag, value);
                }
            }
        });
        beanStream.print();
//        beanStream.getSideOutput(errorTag).print();

        // 使用高德API获取地理位置信息
//        int capacity = 6;
//        SingleOutputStreamOperator<DataLogBean> result = AsyncDataStream.orderedWait(
//                beanStream, //输入的数据流
//                new LocationAsyncAMapFunction(capacity), //异步查询的Function实例
//                300000, //超时时间
//                TimeUnit.MILLISECONDS, //时间单位
//                capacity).setParallelism(4);//异步请求队列最大的数量，不传该参数默认值为100
//
//        result.addSink(new MySQLTwoPhaseCommitSink()).setParallelism(4);

        FlinkUtils.env.execute(Thread.currentThread().getStackTrace()[0].getClassName());
    }

}
