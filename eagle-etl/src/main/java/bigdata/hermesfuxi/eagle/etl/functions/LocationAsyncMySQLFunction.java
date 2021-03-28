package bigdata.hermesfuxi.eagle.etl.functions;

import bigdata.hermesfuxi.eagle.etl.bean.DataLogBean;
import bigdata.hermesfuxi.eagle.etl.utils.DBConnectUtils;
import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class LocationAsyncMySQLFunction extends RichAsyncFunction<DataLogBean, DataLogBean> {
    private transient DruidDataSource druidDataSource;
    private transient ScheduledExecutorService executorService;
    private int maxConnTotal; //线程池最大线程数量

    public LocationAsyncMySQLFunction(int maxConnTotal) {
        this.maxConnTotal = maxConnTotal;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("async-mysql-pool-%d").daemon(true).build());

        druidDataSource = DBConnectUtils.getDataSource();
        druidDataSource.setMaxActive(maxConnTotal);
    }

    @Override
    public void close() throws Exception {
        //关闭数据库连接池
//        druidDataSource.close();
        //关闭线程池
        executorService.shutdown();
    }

    @Override
    public void asyncInvoke(DataLogBean bean, ResultFuture<DataLogBean> resultFuture) throws Exception {
        //调用线程池的submit方法，将查询请求丢入到线程池中异步执行，返回Future对象
        Future<DataLogBean> future = executorService.submit(new Callable<DataLogBean>() {
            @Override
            public DataLogBean call() throws Exception {
                return queryFromMysql(bean);
            }
        });
        CompletableFuture.supplyAsync(new Supplier<DataLogBean>() {
            @Override
            public DataLogBean get() {
                try {
                    return future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).thenAccept(new Consumer<DataLogBean>() {
            @Override
            public void accept(DataLogBean bean) {
                resultFuture.complete(Collections.singleton(bean));
            }
        });
    }

    private DataLogBean queryFromMysql(DataLogBean bean) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = druidDataSource.getConnection();
            ps = connection.prepareStatement("SELECT province, city, district FROM geo_hash_area WHERE geo_hash = ?");
            ps.setString(1, bean.getGeoHashCode());
            rs = ps.executeQuery();
            while (rs.next()){
                String province = rs.getString("province");
                String city = rs.getString("city");
                String district = rs.getString("district");
                bean.setProvince(province);
                bean.setCity(city);
                bean.setDistrict(district);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            DBConnectUtils.close(rs);
            DBConnectUtils.close(ps);
            DBConnectUtils.close(connection);
        }
        return bean;
    }
}
