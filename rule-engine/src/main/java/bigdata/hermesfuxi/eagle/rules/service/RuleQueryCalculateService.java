package bigdata.hermesfuxi.eagle.rules.service;

import bigdata.hermesfuxi.eagle.rules.pojo.RuleParam;

public interface RuleQueryCalculateService<T> {

    public Boolean ruleQueryCalculate(T t, RuleParam ruleParam) throws Exception;

}
