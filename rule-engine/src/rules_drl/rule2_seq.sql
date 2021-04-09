SELECT
    deviceId,
    sequenceMatch('.*(?1).*(?2).*')(
      toDateTime(`timeStamp`),
      eventId = 'A' and properties['p1']='v1',
      eventId = 'C' and properties['p2']='v2'
  ) as isMatch2,
    sequenceMatch('.*(?1).*')(
      toDateTime(`timeStamp`),
      eventId = 'A' and properties['p1']='v1',
      eventId = 'C' and properties['p2']='v2'
    ) as isMatch1
from eagle_detail
where
     deviceId = ?
  and
     timeStamp BETWEEN ? AND ?
  and
    (
      (eventId='A' and properties['p1']='v1')
      or (eventId = 'C' and properties['p2']='v2')
    )
group by deviceId;