package rule.calculate;

import bigdata.hermesfuxi.eagle.rule.pojo.AtomicRuleParam;
import bigdata.hermesfuxi.eagle.rule.pojo.RuleParam;
import bigdata.hermesfuxi.eagle.rule.service.offline.UserActionSequenceQueryServiceClickhouseImpl;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author hermesfuxi
 * desc 用户行为路径类匹配查询测试
 */
public class ActionSequenceQueryClickhouseTest {
    public static void main(String[] args) throws Exception {

        String sql = "SELECT\n" +
                "  deviceId,\n" +
                "  sequenceMatch('.*(?1).*(?2).*(?3)')(\n" +
                "    toDateTime(`timeStamp`),\n" +
                "    eventId = 'Y' and properties['p1']='vy',\n" +
                "    eventId = 'B' and properties['p6']='v4',\n" +
                "    eventId = 'O' and properties['p1']='v9'\n" +
                "  ) as isMatch3,\n" +
                "  \n" +
                "  sequenceMatch('.*(?1).*(?2).*')(\n" +
                "    toDateTime(`timeStamp`),\n" +
                "    eventId = 'Y' and properties['p1']='vy',\n" +
                "    eventId = 'B' and properties['p6']='v4',\n" +
                "    eventId = 'O' and properties['p1']='v9'\n" +
                "  ) as isMatch2,\n" +
                "  \n" +
                "  sequenceMatch('.*(?1).*')(\n" +
                "    toDateTime(`timeStamp`),\n" +
                "    eventId = 'Y' and properties['p1']='vy',\n" +
                "    eventId = 'B' and properties['p6']='v4',\n" +
                "    eventId = 'O' and properties['p1']='v9'\n" +
                "  ) as isMatch1\n" +
                "\n" +
                "from eagle_detail\n" +
                "where  \n" +
                "  deviceId = '000001' \n" +
                "    and \n" +
                "  timeStamp >= 0\n" +
                "    and \n" +
                "  timeStamp <= 5235295739479\n" +
                "    and \n" +
                "  (\n" +
                "        (eventId='Y' and properties['p1']='vy')\n" +
                "     or (eventId = 'B' and properties['p6']='v4')\n" +
                "     or (eventId = 'O' and properties['p1']='v9')\n" +
                "  )\n" +
                "group by deviceId;";


        // 构造一个序列条件
        AtomicRuleParam param1 = new AtomicRuleParam();
        param1.setEventId("Y");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1","v1");
        param1.setProperties(paramProps1);
        param1.setRangeStart(0);
        param1.setRangeEnd(Long.MAX_VALUE);

        AtomicRuleParam param2 = new AtomicRuleParam();
        param2.setEventId("B");
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p6","v4");
        param2.setProperties(paramProps2);
        param2.setRangeStart(0);
        param2.setRangeEnd(Long.MAX_VALUE);



        AtomicRuleParam param3 = new AtomicRuleParam();
        param3.setEventId("O");
        HashMap<String, String> paramProps3 = new HashMap<>();
        paramProps3.put("p1","v9");
        param3.setProperties(paramProps3);
        param3.setRangeStart(0);
        param3.setRangeEnd(Long.MAX_VALUE);

        ArrayList<AtomicRuleParam> ruleParams = new ArrayList<>();
        ruleParams.add(param1);
        ruleParams.add(param2);
        ruleParams.add(param3);


        RuleParam ruleParam = new RuleParam();
        ruleParam.setUserActionSequenceParams(ruleParams);
        ruleParam.setActionSequenceQuerySql(sql);


        UserActionSequenceQueryServiceClickhouseImpl impl = new UserActionSequenceQueryServiceClickhouseImpl();

        boolean b = impl.ruleQueryCalculate("000001", ruleParam);
        System.out.println(ruleParam.getUserActionSequenceQueriedMaxStep());
        System.out.println(b);

    }

}
