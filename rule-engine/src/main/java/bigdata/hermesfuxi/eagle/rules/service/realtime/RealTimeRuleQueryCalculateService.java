package bigdata.hermesfuxi.eagle.rules.service.realtime;

import bigdata.hermesfuxi.eagle.rules.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rules.service.RuleQueryCalculateService;

/**
 * @author Hermesfuxi
 * desc: Flink实时计算： 查询逻辑与判断逻辑分开
 */
public interface RealTimeRuleQueryCalculateService extends RuleQueryCalculateService<Iterable<LogBean>> {

}
