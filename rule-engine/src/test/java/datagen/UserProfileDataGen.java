package datagen;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author hermesfuxi
 * @desc 用户画像数据模拟器
 *  deviceid,k1=v1
 *
 * hbase中需要先创建好画像标签表
 * [root@hdp01 ~]# hbase shell
 * hbase> create 'eagle.user_profile','f'
 */
public class UserProfileDataGen {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop-master:2181,hadoop-master2:2181,hadoop-slave1:2181,hadoop-slave2:2181,hadoop-slave3:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("eagle.user_profile"));

        for (int i = 1; i < 1000; i++) {
            ArrayList<Put> puts = new ArrayList<>();
            // 攒满20条一批
            for (int j = 0; j < 50; j++) {
                // 生成一个用户的画像标签数据
                String deviceId = StringUtils.leftPad(i + "", 3, "0");
                Put put = new Put(Bytes.toBytes(deviceId));
                for (int k = 1; k <= 10; k++) {
                    String key = "tag" + k;
                    String value = "v" + RandomUtils.nextInt(1, 10);
                    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(key), Bytes.toBytes(value));
                }
                // 将这一条画像数据，添加到list中
                puts.add(put);
            }
            // 提交一批
            table.put(puts);
            puts.clear();
        }
        conn.close();
    }
}
