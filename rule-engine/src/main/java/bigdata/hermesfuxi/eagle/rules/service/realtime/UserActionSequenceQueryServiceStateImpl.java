package bigdata.hermesfuxi.eagle.rules.service.realtime;

import bigdata.hermesfuxi.eagle.rules.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rules.pojo.AtomicRuleParam;
import bigdata.hermesfuxi.eagle.rules.pojo.RuleParam;
import bigdata.hermesfuxi.eagle.rules.utils.RuleCalculateUtils;

import java.util.List;

/**
 * @author hermesfuxi
 * desc 用户行为次序类条件查询服务实现（在state中查询）
 */
public class UserActionSequenceQueryServiceStateImpl implements RealTimeRuleQueryCalculateService {


    /**
     * 查询规则条件中的 行为序列条件
     * 会将查询到的最大匹配步骤，set回 ruleParam对象中
     *
     * @param logBeans  用户事件明细
     * @param ruleParam 规则参数对象
     * @return 条件成立与否
     */
    @Override
    public Boolean ruleQueryCalculate(Iterable<LogBean> logBeans, RuleParam ruleParam) throws Exception {
// 则，继续判断行为次序条件：  依次做过：  W(p1=v4) ->   R(p2=v3) -> F
        List<AtomicRuleParam> userActionSequenceParams = ruleParam.getUserActionSequenceParams();
        int maxStep = ruleParam.getUserActionSequenceQueriedMaxStep();
        int nextIndex = ruleParam.getUserActionSequenceQueriedNextStepIndex();

        if (maxStep == userActionSequenceParams.size()) {
            ruleParam.setUserActionSequenceQueriedNextStepIndex(nextIndex + 1);
            return true;
        } else {
            int index = -1;
            for (LogBean logBean : logBeans) {
                index++;
                if (index >= nextIndex && maxStep <= userActionSequenceParams.size() - 1) {
                    boolean flag = RuleCalculateUtils.eventBeanMatchEventParam(logBean, userActionSequenceParams.get(maxStep), true);
                    if (flag) {
                        maxStep++;
                        ruleParam.setUserActionSequenceQueriedMaxStep(maxStep);
                    }
                }
            }
            // 找到了，记录下标，并进入下一个行为次序
            ruleParam.setUserActionSequenceQueriedNextStepIndex(index + 1);
            return maxStep == userActionSequenceParams.size();
        }
    }
}
