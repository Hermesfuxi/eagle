package datagen;

import bigdata.hermesfuxi.eagle.rules.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rules.utils.RandomDataUtils;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Properties;

/**
 * @author hermesfuxi
 * desc 行为日志生成模拟器
 */
public class ActionLogGen {
    public static void main(String[] args) throws InterruptedException {

        String[] carrierArr = {"中国电信", "中国移动", "中国联通"};
        String[] osNameArr = {"android", "ios", "windows", "macos", "linux"};
        String[] netTypeArr = {"3G", "4G", "5G", "WIFI"};
        String[] releaseChannelArr = {"AppStore", "pp助手", "安软市场", "手机乐园", "Google Play store", "腾讯手机", "百度手机助手", "木蚂蚁安卓应用市场", "纽扣助手", "奇珀市场"};
        String[] resolutionArr = {"480*320", "800*480", "854*480", "960*540", "1024*600", "1024*768", "1184*720", "1196*720", "1280*720", "1776*1080", "1812*1080", "1920*1080", "2560*1440"};
        String[] deviceTypeArr = {"IPHONE-7PLUS", "HUAWEI-RY-10", "REDMI-5", "MATE-X",  "MI-6", "MI-7", "MI-10"};
        // 创建多个线程，并行执行
        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {

                @Override
                public void run() {

                    Properties props = new Properties();
                    props.setProperty("bootstrap.servers", "hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092");
                    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    // 构造一个kafka生产者客户端
                    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

                    while (true) {
                        LogBean logBean = new LogBean();
                        // 生成的账号形如： 004078
                        String account = StringUtils.leftPad(RandomUtils.nextInt(1, 1000) + "", 3, "0");
                        logBean.setAccount(account);
                        String appId = StringUtils.leftPad(RandomUtils.nextInt(1, 100) + "", 2, "0");
                        logBean.setAppId(appId);
                        logBean.setAppVersion(String.format("%.1f", RandomUtils.nextDouble(1.0, 10.0)));
                        logBean.setCarrier(carrierArr[RandomUtils.nextInt(0, 3)]);
                        // deviceid直接用account
                        logBean.setDeviceId(account);
                        RandomDataUtils.getRandomIp();
                        logBean.setIp(RandomDataUtils.getRandomIp());
                        logBean.setLatitude(RandomUtils.nextDouble(10.0, 52.0));
                        logBean.setLongitude(RandomUtils.nextDouble(120.0, 160.0));
                        logBean.setDeviceType(deviceTypeArr[RandomUtils.nextInt(0, 7)]);
                        logBean.setNetType(netTypeArr[RandomUtils.nextInt(0, 4)]);
                        logBean.setOsName(osNameArr[RandomUtils.nextInt(0, 5)]);
                        logBean.setOsVersion(String.format("%.1f", RandomUtils.nextDouble(1.0, 20.0)));
                        logBean.setReleaseChannel(releaseChannelArr[RandomUtils.nextInt(0, 10)]);
                        logBean.setResolution(resolutionArr[RandomUtils.nextInt(0, 13)]);
                        logBean.setEventId(RandomStringUtils.randomAlphabetic(1).toUpperCase());

                        HashMap<String, String> properties = new HashMap<String, String>();
                        for (int i = 0; i < RandomUtils.nextInt(1, 5); i++) {
                            // 生成的属性形如：  p1=v3, p2=v5, p3=v3,......
                            properties.put("p" + RandomUtils.nextInt(1, 10), "v" + RandomUtils.nextInt(1, 10));
                        }

                        logBean.setProperties(properties);
                        logBean.setTimeStamp(System.currentTimeMillis());
                        logBean.setSessionId(RandomStringUtils.randomNumeric(10, 10));


                        // 将日志对象，转成JSON
                        String log = JSON.toJSONString(logBean);
                        // 打印在控制台
//                         System.out.println(log);
                        // 写入kafka的topic： eagle-app-log

                        ProducerRecord<String, String> record = new ProducerRecord<>("eagle-app-log", log);
                        kafkaProducer.send(record);

                        try {
                            Thread.sleep(RandomUtils.nextInt(100, 500));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }


            }).start();
        }
    }
}
/*
kafka中要先创建好topic
bin/kafka-topics.sh --create --topic eagle-app-log --partitions 5 --replication-factor 1 --zookeeper localhost:2181

创建完后，检查一下是否创建成功：
bin/kafka-topics.sh --list --zookeeper localhost:2181

JSON 数据如下：
{
	"account": "Vz54E9Ya",
	"appId": "cn.doitedu.app1",
	"appVersion": "3.4",
	"carrier": "中国移动",
	"deviceId": "WEISLD0235S0934OL",
	"deviceType": "MI-6",
    "ip": "24.93.136.175",
	"latitude": 42.09287620431088,
	"longitude": 79.42106825764643,
	"netType": "WIFI",
	"osName": "android",
	"osVersion": "6.5",
	"releaseChannel": "豌豆荚",
	"resolution": "1024*768",
	"sessionId": "SE18329583458",
	"timeStamp": 1594534406220
	"eventId": "productView",
	"properties": {
		"pageId": "646",
		"productId": "157",
		"refType": "4",
		"refUrl": "805",
		"title": "爱得堡 男靴中高帮马丁靴秋冬雪地靴 H1878 复古黄 40码",
		"url": "https://item.jd.com/36506691363.html",
		"utm_campain": "4",
		"utm_loctype": "1",
		"utm_source": "10"
       }
}
 */
