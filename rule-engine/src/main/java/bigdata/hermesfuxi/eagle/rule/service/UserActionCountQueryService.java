package bigdata.hermesfuxi.eagle.rule.service;

import bigdata.hermesfuxi.eagle.rule.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rule.pojo.RuleParam;

/**
 * @author hermesfuxi
 * desc 用户行为次数类条件查询服务接口
 */
public interface UserActionCountQueryService {

    public boolean queryActionCounts(Iterable<LogBean> logBeans, RuleParam ruleParam) throws Exception;
}
