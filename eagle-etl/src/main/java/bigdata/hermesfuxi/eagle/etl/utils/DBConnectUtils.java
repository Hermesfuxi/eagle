package bigdata.hermesfuxi.eagle.etl.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DBConnectUtils {
    private static final Logger logger = Logger.getLogger(DBConnectUtils.class);
    private static DruidDataSource dataSource;

    static {
        Properties properties = new Properties();
        try {
            properties.load(DBConnectUtils.class.getClassLoader().getResourceAsStream("druid.properties"));
            dataSource = (DruidDataSource)DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接池对象
     */
    public static DruidDataSource getDataSource() {
        return dataSource;
    }

    /**
     * 获取数据库连接对象
     */
    public static Connection getConnection() throws Exception {
        return dataSource.getConnection();
    }

    /**
     * 提交事务
     */
    public static void commit(Connection conn) {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {

                logger.error("提交事务失败,Connection:" + conn);
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * 事务回滚
     *
     * @param conn
     */
    public static void rollback(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                logger.error("事务回滚失败,Connection:" + conn);
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * 归还连接
     *
     * @param t   要被归还到熟即可连接池对象的数据库连接对象
     * @param <T> 数据库连接对象的类型
     */
    public static <T> void close(T t) {
        if (t != null) {
            try {
                // 利用反射，获取class对象
                Class<?> aClass = t.getClass();
                // 获取class对象中的方法对象
                Method close = aClass.getMethod("close");
                // 执行方法
                close.invoke(t);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
