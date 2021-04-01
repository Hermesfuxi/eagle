-- 创建事件明细表
set allow_experimental_map_type = 1;
drop table if exists default.eagle_detail;
create table if not exists default.eagle_detail
(
    account           String   ,
    appId             String   ,
    appVersion        String   ,
    carrier           String   ,
    deviceId          String   ,
    deviceType        String   ,
    eventId           String   ,
    ip                String   ,
    latitude          Float64  ,
    longitude         Float64  ,
    netType           String   ,
    osName            String   ,
    osVersion         String   ,
    properties        Map(String,String),
    releaseChannel    String,
    resolution        String,
    sessionId         String,
    timeStamp         Int64 ,
    INDEX u (deviceId) TYPE minmax GRANULARITY 3,
    INDEX t (timeStamp) TYPE minmax GRANULARITY 3
) ENGINE = MergeTree()
ORDER BY (deviceId,timeStamp)
;

-- 创建kafka引擎表
set allow_experimental_map_type = 1;
drop table if exists default.eagle_detail_kafka;
create table if not exists default.eagle_detail_kafka
(
    account           String   ,
    appId             String   ,
    appVersion        String   ,
    carrier           String   ,
    deviceId          String   ,
    deviceType        String   ,
    eventId           String   ,
    ip                String   ,
    latitude          Float64  ,
    longitude         Float64  ,
    netType           String   ,
    osName            String   ,
    osVersion         String   ,
    properties        Map(String,String),
    releaseChannel    String,
    resolution        String,
    sessionId         String,
    timeStamp         Int64
) ENGINE = Kafka('hadoop-master:9092,hadoop-master2:9092,hadoop-slave1:9092,hadoop-slave2:9092,hadoop-slave3:9092','eagle-app-log','group1','JSONEachRow');


-- 创建物化视图
create MATERIALIZED VIEW eagle_view TO eagle_detail
as
select
    account        ,
    appId          ,
    appVersion     ,
    carrier        ,
    deviceId       ,
    deviceType     ,
    eventId        ,
    ip             ,
    latitude       ,
    longitude      ,
    netType        ,
    osName         ,
    osVersion      ,
    properties     ,
    releaseChannel  ,
    resolution      ,
    sessionId       ,
    timeStamp
from eagle_detail_kafka
;
