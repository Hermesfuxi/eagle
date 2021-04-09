select
    deviceId,
    count(1) as cnt
from eagle_detail
where deviceId = ? and eventId = 'B' and properties['p1']='v1'
  and timeStamp between ? and ?
group by deviceId
;
select
    deviceId,
    count(1) as cnt
from eagle_detail
where deviceId = ? and eventId = 'D' and properties['p2']='v2'
  and timeStamp between ? and ?
group by deviceId
;