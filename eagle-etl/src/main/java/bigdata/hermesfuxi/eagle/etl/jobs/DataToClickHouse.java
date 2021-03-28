package bigdata.hermesfuxi.eagle.etl.jobs;

import bigdata.hermesfuxi.eagle.etl.bean.DataLogBean;
import bigdata.hermesfuxi.eagle.etl.functions.LocationAsyncAMapFunction;
import bigdata.hermesfuxi.eagle.etl.utils.FlinkUtils;
import bigdata.hermesfuxi.eagle.etl.utils.MyKafkaDeserializationSchema;
import ch.hsr.geohash.GeoHash;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static bigdata.hermesfuxi.eagle.etl.constant.Constants.GEO_HASH_LENGTH;


/**
 * 将数据存储在 ClickHouse 中
 * @author Hermesfuxi-PC
 */
public class DataToClickHouse {
    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> kafkaSourceV2 = FlinkUtils.getKafkaSourceV2(args, MyKafkaDeserializationSchema.class);
//        kafkaSourceV2.print();

        //解析数据
        SingleOutputStreamOperator<DataLogBean> beanStream = kafkaSourceV2.process(new ProcessFunction<Tuple2<String, String>, DataLogBean>() {
            @Override
            public void processElement(Tuple2<String, String> value, Context ctx, Collector<DataLogBean> out) throws Exception {
                try {
                    DataLogBean dataLogBean = JSON.parseObject(value.f1, DataLogBean.class);
                    if (dataLogBean != null) {
                        dataLogBean.setGid(value.f0);
                        if (dataLogBean.getLatitude() != null && dataLogBean.getLongitude() != null) {
                            Double latitude = dataLogBean.getLatitude();
                            Double longitude = dataLogBean.getLongitude();
                            String geoHashCode = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, GEO_HASH_LENGTH);
                            dataLogBean.setGeoHashCode(geoHashCode);
                        }

                        if(dataLogBean.getTimestamp()!= null){
                            LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(dataLogBean.getTimestamp() / 1000, 0, ZoneOffset.ofHours(8));
                            String date = localDateTime.toLocalDate().toString();
                            dataLogBean.setDate(date);
                            dataLogBean.setHour(localDateTime.getHour());
                        }
                        out.collect(dataLogBean);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
//        beanStream.print();

        int capacity = 10;
        SingleOutputStreamOperator<DataLogBean> result = AsyncDataStream.orderedWait(
                beanStream, //输入的数据流
                new LocationAsyncAMapFunction(capacity), //异步查询的Function实例
                300000, //超时时间
                TimeUnit.MILLISECONDS, //时间单位
                capacity).setParallelism(4);//异步请求队列最大的数量，不传该参数默认值为100

        result.print();

        result.addSink(JdbcSink.sink(
                "insert into doit.tb_user_event (gid,carrier,deviceId,deviceType,eventId,isNew,country,province,city,district,netType,osName,osVersion,releaseChannel,resolution,sessionId,timestamp,date,hour,processTime) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ps, bean) -> {
                    ps.setString(1, bean.getGid());
                    ps.setString(2, bean.getCarrier());
                    ps.setString(3, bean.getDeviceId());
                    ps.setString(4, bean.getDeviceType());
                    ps.setString(5, bean.getEventId());
                    ps.setInt(6, bean.getIsNew());
                    ps.setString(7, bean.getCountry());
                    ps.setString(8, bean.getProvince());
                    ps.setString(9, bean.getCity());
                    ps.setString(10, bean.getDistrict());
                    ps.setString(11, bean.getNetType());
                    ps.setString(12, bean.getOsName());
                    ps.setString(13, bean.getOsVersion());
                    ps.setString(14, bean.getReleaseChannel());
                    ps.setString(15, bean.getResolution());
                    ps.setString(16, bean.getSessionId());
                    ps.setLong(17, bean.getTimestamp());
                    ps.setString(18, bean.getDate());
                    ps.setInt(19, bean.getHour());
                    ps.setString(20, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://hadoop-master:8123/doit")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()));

        FlinkUtils.env.execute();
    }
}
/* clickhouse 建表语句：

create table doit.tb_user_event(
gid String,
carrier String,
deviceId String,
deviceType String,
eventId String,
isNew UInt8,
country String,
province String,
city String,
district String,
netType String,
osName String,
osVersion String,
releaseChannel String,
resolution String,
sessionId String,
timestamp UInt16,
date String,
hour UInt8,
processTime DateTime comment '插入到数据库时的系统时间'
)engine = ReplacingMergeTree(processTime)
partition by (date, hour)
order by gid;

 */
