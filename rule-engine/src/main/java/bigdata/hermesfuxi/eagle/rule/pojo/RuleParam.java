package bigdata.hermesfuxi.eagle.rule.pojo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * @author hermesfuxi
 * desc 规则整体条件封装实体
 *
 *     需求中要实现的判断规则：
 *     触发条件：E事件
 *     画像属性条件：  k3=v3 , k100=v80 , k230=v360
 *     行为属性条件：  U(p1=v3,p2=v2) >= 3次 且  G(p6=v8,p4=v5,p1=v2)>=1
 *     行为次序条件：  依次做过：  W(p1=v4) ->   R(p2=v3) -> F
 *
 */
public class RuleParam implements Serializable {
    // 规则ID
    private String ruleId;

    // 规则中的触发条件
    private AtomicRuleParam triggerParam;

    // 规则中的用户画像条件
    private HashMap<String,String> userProfileParams;

    // 规则中的行为次数类条件
    private List<AtomicRuleParam> userActionCountParams;

    // 规则中的行为次序类条件
    private List<AtomicRuleParam> userActionSequenceParams;

    // 序列模式匹配查询sql
    private String actionSequenceQuerySql;

    // 用于记录查询服务所返回的序列中匹配的最大步骤号
    private int userActionSequenceQueriedMaxStep;

    // 用于记录查询服务所返回的序列中匹配的最大步骤号
    private int userActionSequenceQueriedNextStepIndex;

    public String getActionSequenceQuerySql() {
        return actionSequenceQuerySql;
    }

    public void setActionSequenceQuerySql(String actionSequenceQuerySql) {
        this.actionSequenceQuerySql = actionSequenceQuerySql;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public AtomicRuleParam getTriggerParam() {
        return triggerParam;
    }

    public void setTriggerParam(AtomicRuleParam triggerParam) {
        this.triggerParam = triggerParam;
    }

    public HashMap<String, String> getUserProfileParams() {
        return userProfileParams;
    }

    public void setUserProfileParams(HashMap<String, String> userProfileParams) {
        this.userProfileParams = userProfileParams;
    }

    public List<AtomicRuleParam> getUserActionCountParams() {
        return userActionCountParams;
    }

    public void setUserActionCountParams(List<AtomicRuleParam> userActionCountParams) {
        this.userActionCountParams = userActionCountParams;
    }

    public List<AtomicRuleParam> getUserActionSequenceParams() {
        return userActionSequenceParams;
    }

    public void setUserActionSequenceParams(List<AtomicRuleParam> userActionSequenceParams) {
        this.userActionSequenceParams = userActionSequenceParams;
    }

    public int getUserActionSequenceQueriedMaxStep() {
        return userActionSequenceQueriedMaxStep;
    }

    public void setUserActionSequenceQueriedMaxStep(int userActionSequenceQueriedMaxStep) {
        this.userActionSequenceQueriedMaxStep = userActionSequenceQueriedMaxStep;
    }

    public int getUserActionSequenceQueriedNextStepIndex() {
        return userActionSequenceQueriedNextStepIndex;
    }

    public void setUserActionSequenceQueriedNextStepIndex(int userActionSequenceQueriedNextStepIndex) {
        this.userActionSequenceQueriedNextStepIndex = userActionSequenceQueriedNextStepIndex;
    }
}
