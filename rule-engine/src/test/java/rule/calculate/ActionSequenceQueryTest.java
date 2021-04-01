package rule.calculate;

import bigdata.hermesfuxi.eagle.rule.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rule.pojo.AtomicRuleParam;
import bigdata.hermesfuxi.eagle.rule.pojo.RuleParam;
import bigdata.hermesfuxi.eagle.rule.service.realtime.UserActionSequenceQueryServiceStateImpl;

import java.util.ArrayList;
import java.util.HashMap;

public class ActionSequenceQueryTest {
    public static void main(String[] args) throws Exception {

        // 构造一些事件
        LogBean logBean1 = new LogBean();
        logBean1.setEventId("010");
        HashMap<String, String> props1 = new HashMap<>();
        props1.put("p1","v1");
        logBean1.setProperties(props1);

        LogBean logBean5 = new LogBean();
        logBean5.setEventId("020");
        HashMap<String, String> props5 = new HashMap<>();
        props5.put("p2","v3");
        logBean5.setProperties(props5);


        LogBean logBean2 = new LogBean();
        logBean2.setEventId("310");
        HashMap<String, String> props2 = new HashMap<>();
        props2.put("p1","v2");
        logBean2.setProperties(props2);


        LogBean logBean3 = new LogBean();
        logBean3.setEventId("020");
        HashMap<String, String> props3 = new HashMap<>();
        props3.put("p2","v3");
        props3.put("p4","v5");
        logBean3.setProperties(props3);


        LogBean logBean4 = new LogBean();
        logBean4.setEventId("022");
        HashMap<String, String> props4 = new HashMap<>();
        props4.put("p2","v3");
        props4.put("p3","v4");
        logBean4.setProperties(props4);


        ArrayList<LogBean> eventList = new ArrayList<>();
        eventList.add(logBean1);
        eventList.add(logBean5);
        eventList.add(logBean2);
        eventList.add(logBean3);
        eventList.add(logBean4);

        // 构造一个序列条件
        AtomicRuleParam param1 = new AtomicRuleParam();
        param1.setEventId("010");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1","v1");
        param1.setProperties(paramProps1);

        AtomicRuleParam param2 = new AtomicRuleParam();
        param2.setEventId("020");
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p2","v2");
        param2.setProperties(paramProps2);

        AtomicRuleParam param3 = new AtomicRuleParam();
        param3.setEventId("020");
        HashMap<String, String> paramProps3 = new HashMap<>();
        paramProps3.put("p4","v5");
        param3.setProperties(paramProps3);

        ArrayList<AtomicRuleParam> ruleSequenceParams = new ArrayList<>();
        ruleSequenceParams.add(param1);
        ruleSequenceParams.add(param2);
        ruleSequenceParams.add(param3);

        // 调用sevice计算
        UserActionSequenceQueryServiceStateImpl service = new UserActionSequenceQueryServiceStateImpl();
        RuleParam ruleParam = new RuleParam();
        ruleParam.setUserActionSequenceParams(ruleSequenceParams);
        boolean flag = service.ruleQueryCalculate(eventList, ruleParam);
        System.out.println(flag);
        System.out.println(ruleParam.getUserActionSequenceQueriedMaxStep());
    }
}
