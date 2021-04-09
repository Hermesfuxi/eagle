package bigdata.hermesfuxi.eagle.rules.service;

import bigdata.hermesfuxi.eagle.rules.pojo.RuleParam;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

/**
 * @author hermesfuxi
 * desc 用户画像查询服务，hbase查询实现类
 */
public class UserProfileQueryServiceHbaseImpl implements UserProfileQueryService {
    private Table table;
    private String family;

    public UserProfileQueryServiceHbaseImpl() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop-master:2181,hadoop-master2:2181,hadoop-slave1:2181,hadoop-slave2:2181,hadoop-slave3:2181");
        try {
            Connection conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf("eagle.user_profile"));
        }catch (Exception e){
            e.printStackTrace();
        }
        family = "f";
    }

    /**
     * 传入一个用户号，以及要查询的条件
     * 返回这些条件是否满足
     */
    @Override
    public boolean judgeProfileCondition(String deviceId, RuleParam ruleParam) throws IOException {
        HashMap<String, String> userProfileParams = ruleParam.getUserProfileParams();
        Set<String> keySet = userProfileParams.keySet();
        // 判断画像属性条件：  k3=v3 , k100=v80 , k230=v360
        // 查询hbase即可
        // 构造查询条件
        Get get = new Get(Bytes.toBytes(deviceId));
        for (String key : keySet) {
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(key));
        }

        Result result = table.get(get);

        for (String key : userProfileParams.keySet()) {
            // 传入查询条件并查询
            byte[] valueBytes = result.getValue(Bytes.toBytes(family), Bytes.toBytes(key));
            if(valueBytes == null || userProfileParams.get(key) == null || !userProfileParams.get(key).equals(new String(valueBytes))){
                return false;
            }
        }

        return true;
    }


}
