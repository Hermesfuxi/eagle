package bigdata.hermesfuxi.eagle.etl.functions;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.ParameterizedType;

public class JsonToBeanFunc<T> extends ProcessFunction<String, T> {

    @Override
    public void processElement(String value, Context ctx, Collector<T> out) throws Exception {
        try{
            Class<T> tClass = (Class<T>)((ParameterizedType)getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            T t = JSON.parseObject(value, tClass);
            if(t != null){
                out.collect(t);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
