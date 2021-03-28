package bigdata.hermesfuxi.eagle.rule.utils;

import bigdata.hermesfuxi.eagle.rule.pojo.RuleAtomicParam;
import bigdata.hermesfuxi.eagle.rule.pojo.RuleParam;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author hermesfuxi
 * desc 规则模拟器
 */
public class RuleSimulator {

    public static RuleParam getRuleParam(){

        RuleParam ruleParam = new RuleParam();
        ruleParam.setRuleId("test_rule_1");

        // 构造触发条件
        RuleAtomicParam trigger = new RuleAtomicParam();
        trigger.setEventId("E");
        ruleParam.setTriggerParam(trigger);


        // 构造画像条件
        HashMap<String, String> userProfileParams = new HashMap<>();
        userProfileParams.put("tag1","v9");
        userProfileParams.put("tag2","v3");
        ruleParam.setUserProfileParams(userProfileParams);


        // 行为次数条件
        RuleAtomicParam count1 = new RuleAtomicParam();
        count1.setEventId("B");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1","v1");
        count1.setProperties(paramProps1);
        count1.setRangeStart(-1);
        count1.setRangeEnd(-1);
        count1.setCnts(1);

//        RuleAtomicParam count2 = new RuleAtomicParam();
//        count2.setEventId("D");
//        HashMap<String, String> paramProps2 = new HashMap<>();
//        paramProps2.put("p2","v3");
//        count2.setProperties(paramProps2);
//        count2.setRangeStart(-1);
//        count2.setRangeEnd(-1);
//        count2.setCnts(1);


        ArrayList<RuleAtomicParam> countParams = new ArrayList<>();
        countParams.add(count1);
//        countParams.add(count2);
        ruleParam.setUserActionCountParams(countParams);


        // 行为序列条件(3个事件的序列）
        RuleAtomicParam param1 = new RuleAtomicParam();
        param1.setEventId("A");
        HashMap<String, String> seqProps1 = new HashMap<>();
        seqProps1.put("p1","v1");
        param1.setProperties(seqProps1);
        param1.setRangeStart(-1);
        param1.setRangeEnd(-1);

        RuleAtomicParam param2 = new RuleAtomicParam();
        param2.setEventId("C");
        HashMap<String, String> seqProps2 = new HashMap<>();
        seqProps2.put("p2","v2");
        param2.setProperties(seqProps2);
        param2.setRangeStart(-1);
        param2.setRangeEnd(-1);


        ArrayList<RuleAtomicParam> ruleParams = new ArrayList<>();
        ruleParams.add(param1);
        ruleParams.add(param2);

        ruleParam.setUserActionSequenceParams(ruleParams);


        return  ruleParam;
    }
}
