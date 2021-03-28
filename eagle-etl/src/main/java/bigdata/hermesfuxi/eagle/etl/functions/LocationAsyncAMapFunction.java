package bigdata.hermesfuxi.eagle.etl.functions;

import bigdata.hermesfuxi.eagle.etl.bean.DataLogBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * 使用高德地图API：根据经纬度查询省市区
 */
public class LocationAsyncAMapFunction extends RichAsyncFunction<DataLogBean, DataLogBean> {
    private transient CloseableHttpAsyncClient httpclient; //异步请求的HttpClient
    private transient Map<String, Tuple4<String, String, String, String>> map; //异步请求的HttpClient
    private String url; //请求高德地图URL地址
    private String key; //请求高德地图的秘钥，注册高德地图开发者后获得
    private int maxConnTotal; //异步HTTPClient支持的最大连接

    public LocationAsyncAMapFunction(int maxConnTotal) {
        this.maxConnTotal = maxConnTotal;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        RequestConfig requestConfig = RequestConfig.custom().build();
        httpclient = HttpAsyncClients.custom() //创建HttpAsyncClients请求连接池
                .setMaxConnTotal(maxConnTotal) //设置最大连接数
                .setDefaultRequestConfig(requestConfig).build();
        httpclient.start(); //启动异步请求httpClient

        Properties properties = new Properties();
        properties.load(LocationAsyncAMapFunction.class.getClassLoader().getResourceAsStream("config.properties"));
        url = properties.getProperty("amap.url");
        key = properties.getProperty("amap.key");

        map = new ConcurrentHashMap<>();
    }

    @Override
    public void asyncInvoke(DataLogBean bean, ResultFuture<DataLogBean> resultFuture) throws Exception {
        double longitude = bean.getLongitude(); //获取经度
        double latitude = bean.getLatitude();; //获取维度
        String geoHashCode = bean.getGeoHashCode();; //获取维度
//        if(map.containsKey(geoHashCode)){
//            // 使用缓存数据
//            Tuple4<String, String, String, String> tuple4 = map.get(geoHashCode);
//            bean.setCountry(tuple4.f0);
//            bean.setProvince(tuple4.f1);
//            bean.setCity(tuple4.f2);
//            bean.setDistrict(tuple4.f3);
//            resultFuture.complete(Collections.singleton(bean));
//        } else {
            //将经纬度和高德地图的key与请求的url进行拼接
            String requestUrl = url + "?key=" + key + "&location=" + longitude + "," + latitude + "&id=" + bean.getId();
            System.out.println(requestUrl);
            HttpGet httpGet = new HttpGet(requestUrl);
            //发送异步请求，返回Future
            Future<HttpResponse> future = httpclient.execute(httpGet, null);
            HttpResponse response = future.get();
            CompletableFuture.supplyAsync(new Supplier<DataLogBean>() {
                @Override
                public DataLogBean get() {
                    try {
                        String country = null;
                        String province = null;
                        String city = null;
                        String district = null;
                        if (response.getStatusLine().getStatusCode() == 200) {
                            //解析返回的结果，获取省份、城市等信息
                            String result = EntityUtils.toString(response.getEntity());
                            JSONObject jsonObj = JSON.parseObject(result);
                            JSONObject regeocode = jsonObj.getJSONObject("regeocode");
                            if (regeocode != null && !regeocode.isEmpty()) {
                                JSONObject address = regeocode.getJSONObject("addressComponent");
                                country = address.getString("country");
                                province = address.getString("province");
                                city = address.getString("city");
                                district = address.getString("district");
                            }
                        }
                        bean.setCountry(country);
                        bean.setProvince(province);
                        bean.setCity(city);
                        bean.setDistrict(district);
                        map.put(bean.getGeoHashCode(), Tuple4.of(country, province, city, district));
                        return bean;
                    } catch (Exception e) {
                        return null;
                    }
                }
            }).thenAccept((DataLogBean result) -> {
                //将结果添加到resultFuture中输出（complete方法的参数只能为集合，如果只有一个元素，就返回一个单例集合）
                resultFuture.complete(Collections.singleton(result));
            });
//        }
    }
}
