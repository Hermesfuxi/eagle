package bigdata.hermesfuxi.eagle.rules.buffer;

import redis.clients.jedis.Jedis;

public class RedisBufferManager {
    Jedis jedis;

    public RedisBufferManager(){
        jedis = new Jedis("localhost", 16379);
    }

    // 获取缓存中的数据
    public BufferResult getBufferData(String bufferKey,long paramRangeStart,long paramRangeEnd,int threshold){

        BufferResult bufferResult = new BufferResult();
        bufferResult.setBufferAvailableLevel(BufferAvailableLevel.UN_AVL);

        // data:   2|t1,t8
        String data = jedis.get(bufferKey);
        String[] split = data.split("\\|");
        String[] timeRange = split[1].split(",");

        bufferResult.setBufferKey(bufferKey);
        bufferResult.setBufferValue(Integer.parseInt(split[0]));
        bufferResult.setBufferRangeStart(Long.parseLong(timeRange[0]));
        bufferResult.setBufferRangeEnd(Long.parseLong(timeRange[1]));

        if(paramRangeStart<= bufferResult.getBufferRangeStart()
                && paramRangeEnd>=bufferResult.getBufferRangeEnd()
                && bufferResult.getBufferValue()>=threshold){

            bufferResult.setBufferAvailableLevel(BufferAvailableLevel.WHOLE_AVL);
        }

        if(paramRangeStart == bufferResult.getBufferRangeStart() && paramRangeEnd>=bufferResult.getBufferRangeEnd()){

            bufferResult.setBufferAvailableLevel(BufferAvailableLevel.PARTIAL_AVL);
            // 更新外部后续查询的条件窗口起始点
            bufferResult.setOutSideQueryStart(bufferResult.getBufferRangeEnd());

        }

        return bufferResult;
    }

    // 插入数据到缓存
    public void putBufferData(String bufferKey,int value,long bufferRangeStart,long bufferRangeEnd){
        jedis.setex(bufferKey,4*60*60*1000,value+"|"+bufferRangeStart+","+bufferRangeEnd);
    }


    // 更新已存在的缓存数据


    // 删除已存在的缓存数据
}
