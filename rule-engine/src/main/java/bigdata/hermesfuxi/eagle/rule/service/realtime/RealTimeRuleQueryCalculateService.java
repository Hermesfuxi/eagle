package bigdata.hermesfuxi.eagle.rule.service.realtime;

import bigdata.hermesfuxi.eagle.rule.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rule.service.RuleQueryCalculateService;

/**
 * @author Hermesfuxi
 * desc: Flink实时计算： 查询逻辑与判断逻辑分开
 */
public interface RealTimeRuleQueryCalculateService extends RuleQueryCalculateService<Iterable<LogBean>> {

}
