package bigdata.hermesfuxi.eagle.rules.pojo;

import org.kie.api.runtime.KieSession;

public class RuleStateBean {
    private String ruleName;
    private KieSession kieSession;
    private RuleParam ruleParam;
    private String ruleType;
    private String routerClass;
    private String cntSqls;
    private String seqSqls;

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public KieSession getKieSession() {
        return kieSession;
    }

    public void setKieSession(KieSession kieSession) {
        this.kieSession = kieSession;
    }

    public RuleParam getRuleParam() {
        return ruleParam;
    }

    public void setRuleParam(RuleParam ruleParam) {
        this.ruleParam = ruleParam;
    }

    public String getRuleType() {
        return ruleType;
    }

    public void setRuleType(String ruleType) {
        this.ruleType = ruleType;
    }

    public String getRouterClass() {
        return routerClass;
    }

    public void setRouterClass(String routerClass) {
        this.routerClass = routerClass;
    }

    public String getCntSqls() {
        return cntSqls;
    }

    public void setCntSqls(String cntSqls) {
        this.cntSqls = cntSqls;
    }

    public String getSeqSqls() {
        return seqSqls;
    }

    public void setSeqSqls(String seqSqls) {
        this.seqSqls = seqSqls;
    }
}
