package rule.calculate;

import bigdata.hermesfuxi.eagle.rules.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rules.pojo.AtomicRuleParam;
import bigdata.hermesfuxi.eagle.rules.pojo.RuleParam;
import bigdata.hermesfuxi.eagle.rules.service.realtime.UserActionCountQueryServiceStateImpl;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author hermesfuxi
 * desc 行为次数查询服务功能测试
 */
public class ActionCountsQueryTest {
    public static void main(String[] args) throws Exception {


        UserActionCountQueryServiceStateImpl service = new UserActionCountQueryServiceStateImpl();

        // 构造几个明细事件
        LogBean logBean1 = new LogBean();
        logBean1.setEventId("010");
        HashMap<String, String> props1 = new HashMap<>();
        props1.put("p1","v1");
        logBean1.setProperties(props1);


        LogBean logBean2 = new LogBean();
        logBean2.setEventId("010");
        HashMap<String, String> props2 = new HashMap<>();
        props2.put("p1","v2");
        logBean2.setProperties(props2);


        LogBean logBean3 = new LogBean();
        logBean3.setEventId("020");
        HashMap<String, String> props3 = new HashMap<>();
        props3.put("p2","v3");
        logBean3.setProperties(props3);


        LogBean logBean4 = new LogBean();
        logBean4.setEventId("020");
        HashMap<String, String> props4 = new HashMap<>();
        props4.put("p2","v3");
        props4.put("p3","v4");
        logBean4.setProperties(props4);


        ArrayList<LogBean> eventList = new ArrayList<>();
        eventList.add(logBean1);
        eventList.add(logBean2);
        eventList.add(logBean3);
        eventList.add(logBean4);


        // 构造2个规则原子条件
        AtomicRuleParam param1 = new AtomicRuleParam();
        param1.setEventId("010");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1","v1");
        param1.setProperties(paramProps1);
        param1.setCnts(2);

        AtomicRuleParam param2 = new AtomicRuleParam();
        param2.setEventId("020");
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p2","v3");
        param2.setProperties(paramProps2);
        param2.setCnts(2);

        ArrayList<AtomicRuleParam> ruleParams = new ArrayList<>();
        ruleParams.add(param1);
        ruleParams.add(param2);

        RuleParam ruleParam = new RuleParam();
        ruleParam.setUserActionCountParams(ruleParams);
        service.ruleQueryCalculate(eventList, ruleParam);

        for (AtomicRuleParam atomicRuleParam : ruleParams) {
            System.out.println(atomicRuleParam.getEventId()+","+ atomicRuleParam.getCnts() + "," + atomicRuleParam.getRealCnts());
        }
    }
}
