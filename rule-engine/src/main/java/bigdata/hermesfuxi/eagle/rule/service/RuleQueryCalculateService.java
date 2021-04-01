package bigdata.hermesfuxi.eagle.rule.service;

import bigdata.hermesfuxi.eagle.rule.pojo.AtomicRuleParam;
import bigdata.hermesfuxi.eagle.rule.pojo.RuleParam;

public interface RuleQueryCalculateService<T> {

    public Boolean ruleQueryCalculate(T t, RuleParam ruleParam) throws Exception;

}
