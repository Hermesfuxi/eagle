package bigdata.hermesfuxi.eagle.rule.service.offline;

import bigdata.hermesfuxi.eagle.rule.pojo.AtomicRuleParam;
import bigdata.hermesfuxi.eagle.rule.pojo.RuleParam;
import bigdata.hermesfuxi.eagle.rule.utils.DBConnectionUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UserActionCountQueryServiceClickhouseImpl implements OfflineRuleQueryCalculateService {


    private final Connection connection;

    public UserActionCountQueryServiceClickhouseImpl() throws Exception {
        // 获取clickhouse的jdbc连接对象
        connection = DBConnectionUtils.getClickhouseConnection();
    }

    /**
     * 根据给定的deviceId，查询这个人是否满足ruleParam中的所有“次数类"规则条件
     *
     * @param deviceId  要查询的用户
     * @param ruleParam 规则参数对象
     * @return 条件查询的结果是否成立
     */
    @Override
    public Boolean ruleQueryCalculate(String deviceId, RuleParam ruleParam) throws Exception {
        List<AtomicRuleParam> userActionCountParams = ruleParam.getUserActionCountParams();
        // 遍历每一个原子条件进行查询判断
        for (AtomicRuleParam atomicRuleParam : userActionCountParams) {
            boolean flag = ruleQueryCalculate(deviceId, atomicRuleParam);
            if (!flag) {
                return false;
            }
        }
        // 如果走到这一句代码，说明上面的每一个原子条件查询后都满足规则，那么返回最终结果true
        return true;
    }

    public Boolean ruleQueryCalculate(String deviceId, AtomicRuleParam atomicRuleParam) throws Exception {
        // 对当前的原子条件拼接查询sql
        String sql = getSql(deviceId, atomicRuleParam);
        System.out.println(sql);

        // 获取一个clickhouse 的jdbc连接
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        /*
         * deviceId,cnt
         *  000001 ,6
         */
        while (resultSet.next()) {
            int realCnt = (int) resultSet.getLong(2);
            atomicRuleParam.setRealCnts(realCnt);
        }

        // 只要有一个原子条件查询结果不满足，则直接返回最终结果false
        return atomicRuleParam.getRealCnts() < atomicRuleParam.getCnts();
    }

    private String getSql(String deviceId, AtomicRuleParam atomicRuleParam) {

        String templet1 = " select " +
                "\n   deviceId,count() as cnt " +
                "\n from eagle_detail " +
                "\n where deviceId= '" + deviceId + "'" +
                "\n   and " +
                "\n  eventId = '" + atomicRuleParam.getEventId() + "' " +
                "\n   and " +
                "\n  timeStamp >= " + atomicRuleParam.getRangeStart() + " and timeStamp <=" + atomicRuleParam.getRangeEnd();

        String templet3 = "\n group by deviceId";

        HashMap<String, String> properties = atomicRuleParam.getProperties();
        Set<Map.Entry<String, String>> entries = properties.entrySet();

        StringBuilder sqlStr = new StringBuilder();
        for (Map.Entry<String, String> entry : entries) {
            // "and properties['pageId'] = 'page006'"
            sqlStr.append("\n   and properties['").append(entry.getKey()).append("'] = '").append(entry.getValue()).append("'");
        }
        return templet1 + sqlStr.toString() + templet3;
    }
}
