-- 查询事件次数: 对应的是 UserActionCountQueryServiceClickhouseImpl 中的逻辑查询
select
    deviceId,
    count(1) as cnt
from event_detail
where deviceId='nJBTQejDxDmc' and eventId='adShow' and properties['adId']='14'
  and timeStamp between 1615900460000 and 1615900580000
group by deviceId
;

-- 事件序列查询sql: 对应的是 UserActionSequenceQueryServiceClickhouseImpl 中的逻辑查询
/*

┌─deviceId─┬─isMatch3─┬─isMatch2─┬─isMatch1─┐
│ 000001   │        0 │        1 │        1 │
└──────────┴──────────┴──────────┴──────────┘
*/

SELECT
  deviceId,
  sequenceMatch('.*(?1).*(?2).*(?3)')(
    toDateTime(`timeStamp`),
    eventId = 'Y' and properties['p1']='v1',
    eventId = 'B' and properties['p6']='v4',
    eventId = 'O' and properties['p1']='vv'
  ) as isMatch3,

  sequenceMatch('.*(?1).*(?2).*')(
    toDateTime(`timeStamp`),
    eventId = 'Y' and properties['p1']='v1',
    eventId = 'B' and properties['p6']='v4',
    eventId = 'O' and properties['p1']='vv'
  ) as isMatch2,

  sequenceMatch('.*(?1).*')(
    toDateTime(`timeStamp`),
    eventId = 'Y' and properties['p1']='v1',
    eventId = 'B' and properties['p6']='v4',
    eventId = 'O' and properties['p1']='vv'
  ) as isMatch1

from eagle_detail
where
  deviceId = '000001'
    and
  timeStamp >= 0
    and
  timeStamp <= 5235295739479
    and
  (
        (eventId='Y' and properties['p1']='v1')
     or (eventId = 'B' and properties['p6']='v4')
     or (eventId = 'O' and properties['p1']='vv')
  )
group by deviceId;