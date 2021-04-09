package bigdata.hermesfuxi.eagle.rules.utils;

import bigdata.hermesfuxi.eagle.rules.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rules.pojo.RuleStateBean;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;

public class StateDescUtil {
    /**
     * 存放drools规则容器session的state定义器
     */
    public static final MapStateDescriptor<String, RuleStateBean> ruleKieStateDesc = new MapStateDescriptor<String, RuleStateBean>("ruleKieState",String.class,RuleStateBean.class);

    public static final ListStateDescriptor<LogBean> eventStateDesc = new ListStateDescriptor<>("eventState", LogBean.class);
}
