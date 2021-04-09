package bigdata.hermesfuxi.eagle.rules.service;

import bigdata.hermesfuxi.eagle.rules.pojo.RuleCanalBean;
import bigdata.hermesfuxi.eagle.rules.pojo.RuleStateBean;
import bigdata.hermesfuxi.eagle.rules.pojo.RuleTableRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.util.List;

@Slf4j
public class RuleOperationHandler {


    /**
     * 规则操作处理入口方法
     *
     * @param ruleCanalBean
     * @param mapState
     */
    public static boolean handleRuleOper(RuleCanalBean ruleCanalBean, BroadcastState<String, RuleStateBean> mapState) {

        try {
            // 从canal信息中，取到规则表的行数据List（id,规则名,规则代码,cntsql,seqsql)
            List<RuleTableRecord> dataList = ruleCanalBean.getData();
            if (dataList == null || dataList.size() < 1) {
                return true;
            }

            // 从行数据List中取到第一行（其实就只有一行）（id,规则名,规则代码,cntsql,seqsql)
            RuleTableRecord ruleTableRecord = dataList.get(0);
            String ruleName = ruleTableRecord.getRule_name();

            // 如果status=1，则做新增规则的操作
            if (ruleTableRecord.getRule_status() == 1) {

                // status =1 表示，有一条规则要使用，则往state中插入该规则信息
                RuleStateBean ruleStateBean = new RuleStateBean();
                ruleStateBean.setRuleName(ruleName);

                // 往statebean中cntsql
                ruleStateBean.setCntSqls(ruleTableRecord.getCnt_sqls());

                // 往statebean中seqsql
                ruleStateBean.setSeqSqls(ruleTableRecord.getSeq_sqls());

                // 往statebean中放kiesession
                KieHelper kieHelper = new KieHelper();
                kieHelper.addContent(ruleTableRecord.getRule_code(), ResourceType.DRL);
                KieSession kieSession = kieHelper.build().newKieSession();

                ruleStateBean.setKieSession(kieSession);


                // 把statebean放入state
                mapState.put(ruleName, ruleStateBean);

            }
            // 否则，只有删除这种情况，则做删除操作
            else {
                mapState.remove(ruleName);
            }
            return true;
        } catch (Exception e) {

            log.error("规则处理出现异常,异常信息: \n {}",e.getMessage());

            return false;
        }
    }

}
