package bigdata.hermesfuxi.eagle.rules.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBConnectionUtils {
    public static Connection getClickhouseConnection() throws Exception {
        //String ckDriver = "com.github.housepower.jdbc.ClickHouseDriver";
        String ckDriver = "ru.yandex.clickhouse.ClickHouseDriver";
        String ckUrl = "jdbc:clickhouse://hadoop-master:8123/default";
        String table = "eagle_detail";

        Class.forName(ckDriver);
        return DriverManager.getConnection(ckUrl);
    }
}
