package rule.calculate;

import bigdata.hermesfuxi.eagle.rule.pojo.AtomicRuleParam;
import bigdata.hermesfuxi.eagle.rule.pojo.RuleParam;
import bigdata.hermesfuxi.eagle.rule.service.offline.UserActionCountQueryServiceClickhouseImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ActionCountsQueryClickhouseTest {

    public static void main(String[] args) throws Exception {

        UserActionCountQueryServiceClickhouseImpl impl = new UserActionCountQueryServiceClickhouseImpl();


        // 构造2个规则原子条件
        AtomicRuleParam param1 = new AtomicRuleParam();
        param1.setEventId("B");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1","v7");
        param1.setRangeStart(0);
        param1.setRangeEnd(Long.MAX_VALUE);
        param1.setProperties(paramProps1);
        param1.setCnts(2);

        AtomicRuleParam param2 = new AtomicRuleParam();
        param2.setEventId("W");
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p2","v3");
        param2.setProperties(paramProps2);
        param2.setRangeStart(0);
        param2.setRangeEnd(Long.MAX_VALUE);
        param2.setCnts(2);

        ArrayList<AtomicRuleParam> ruleParams = new ArrayList<>();
        ruleParams.add(param1);
        ruleParams.add(param2);

        RuleParam ruleParam = new RuleParam();
        ruleParam.setUserActionCountParams(ruleParams);


        boolean b = impl.ruleQueryCalculate("000001", ruleParam);
        List<AtomicRuleParam> params = ruleParam.getUserActionCountParams();
        for (AtomicRuleParam param : params) {
            System.out.println(param.getCnts() + ", " + param.getRealCnts());
        }


        System.out.println(b);



    }
}
