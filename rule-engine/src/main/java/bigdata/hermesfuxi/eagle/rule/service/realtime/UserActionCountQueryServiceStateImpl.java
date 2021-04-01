package bigdata.hermesfuxi.eagle.rule.service.realtime;

import bigdata.hermesfuxi.eagle.rule.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rule.pojo.AtomicRuleParam;
import bigdata.hermesfuxi.eagle.rule.pojo.RuleParam;
import bigdata.hermesfuxi.eagle.rule.utils.RuleCalculateUtils;

import java.util.List;

/**
 * @author hermesfuxi
 * desc 用户行为次数类条件查询服务实现：在flink的state中统计行为次数
 */
public class UserActionCountQueryServiceStateImpl implements RealTimeRuleQueryCalculateService {


    /**
     * 查询规则参数对象中，要求的用户行为次数类条件是否满足
     * 同时，将查询到的真实次数，set回 规则参数对象中
     *
     * @param logBeans 用户事件明细
     * @param ruleParam  规则整体参数对象
     * @return 条件是否满足
     */
    @Override
    public Boolean ruleQueryCalculate(Iterable<LogBean> logBeans, RuleParam ruleParam) throws Exception {
        // 判断行为次数条件：  B(p1=v1) >= 1次 且  D(p2=v3)>=1
        List<AtomicRuleParam> userActionCountParams = ruleParam.getUserActionCountParams();

        for (AtomicRuleParam userActionCountParam : userActionCountParams) {
            // B(p1=v1) >= 1次 且  D(p2=v3)>=1
            // 内循环，遍历每一个历史明细事件，看看能否找到与当前条件匹配的事件
            int count = userActionCountParam.getRealCnts();
            for (LogBean logBean : logBeans) {
                boolean flag = RuleCalculateUtils.eventBeanMatchEventParam(logBean, userActionCountParam, true);
                if(flag){
                    count++;
                }
            }
            userActionCountParam.setRealCnts(count);
            if(count < userActionCountParam.getCnts()){
                return false;
            }
        }
        return true;
    }
}
