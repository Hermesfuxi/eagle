package bigdata.hermesfuxi.eagle.rules.pojo;

import bigdata.hermesfuxi.eagle.rules.service.router.QueryRouterV4;

public class DroolFact {
    private LogBean logBean;

    private RuleParam ruleParam;

    private QueryRouterV4 queryRouterV4;

    private boolean match;

    public DroolFact() {
    }

    public DroolFact(LogBean logBean, RuleParam ruleParam, QueryRouterV4 queryRouterV4, boolean match) {
        this.logBean = logBean;
        this.ruleParam = ruleParam;
        this.queryRouterV4 = queryRouterV4;
        this.match = match;
    }

    public LogBean getLogBean() {
        return logBean;
    }

    public void setLogBean(LogBean logBean) {
        this.logBean = logBean;
    }

    public RuleParam getRuleParam() {
        return ruleParam;
    }

    public void setRuleParam(RuleParam ruleParam) {
        this.ruleParam = ruleParam;
    }

    public QueryRouterV4 getQueryRouterV4() {
        return queryRouterV4;
    }

    public void setQueryRouterV4(QueryRouterV4 queryRouterV4) {
        this.queryRouterV4 = queryRouterV4;
    }

    public boolean isMatch() {
        return match;
    }

    public void setMatch(boolean match) {
        this.match = match;
    }
}
