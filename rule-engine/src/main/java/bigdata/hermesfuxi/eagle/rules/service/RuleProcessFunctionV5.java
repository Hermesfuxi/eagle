package bigdata.hermesfuxi.eagle.rules.service;

import bigdata.hermesfuxi.eagle.rules.pojo.*;
import bigdata.hermesfuxi.eagle.rules.service.router.QueryRouterV4;
import bigdata.hermesfuxi.eagle.rules.utils.StateDescUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.kie.api.runtime.KieSession;

import java.util.ArrayList;
import java.util.Map;

/**
 * @author hermesfuxi
 * desc 规则核心处理函数版本4.0
 */
@Slf4j
public class RuleProcessFunctionV5 extends KeyedBroadcastProcessFunction<String, LogBean, String, ResultBean> {

    QueryRouterV4 queryRouterV4;

    ListState<LogBean> eventState;

    // RuleParam ruleParam;

    @Override
    public void open(Configuration parameters) throws Exception {

        /*
         * 准备一个存储明细事件的state
         * 控制state的ttl周期为最近2小时
         */

        ListStateDescriptor<LogBean> eventStateDesc = StateDescUtil.eventStateDesc;
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).updateTtlOnCreateAndWrite().build();
        eventStateDesc.enableTimeToLive(ttlConfig);
        eventState = getRuntimeContext().getListState(eventStateDesc);

        // 构造一个查询路由控制器
        queryRouterV4 = new QueryRouterV4();
        queryRouterV4.setEventState(eventState);

    }


    /**
     * 规则计算核心方法
     *
     * @param logBean 事件bean
     * @param ctx     上下文
     * @param out     输出
     * @throws Exception 异常
     */
    @Override
    public void processElement(LogBean logBean, ReadOnlyContext ctx, Collector<ResultBean> out) throws Exception {


        ReadOnlyBroadcastState<String, RuleStateBean> ruleState = ctx.getBroadcastState(StateDescUtil.ruleKieStateDesc);

        // 将收到的事件放入历史明细state存储中
        // 超过2小时的logBean会被自动清除（前面设置了ttl存活时长）
        eventState.add(logBean);

        Iterable<Map.Entry<String, RuleStateBean>> entries = ruleState.immutableEntries();
        for (Map.Entry<String, RuleStateBean> entry : entries) {

            String ruleName = entry.getKey();
            RuleStateBean stateBean = entry.getValue();

            // 从rulestate中取sql
            String cntSqls = stateBean.getCntSqls();
            String seqSqls = stateBean.getSeqSqls();

            // 从rulestate中取出kiesession
            KieSession kieSession = stateBean.getKieSession();

            // 构造ruleparam对象
            RuleParam ruleParam = new RuleParam();


            // 放入cntsql
            String[] cntSqlArr = cntSqls.split(";");
            ArrayList<AtomicRuleParam> countParams = new ArrayList<>();
            for (String cntSql : cntSqlArr) {
                AtomicRuleParam ruleAtomicParam = new AtomicRuleParam();
                ruleAtomicParam.setCountQuerySql(cntSql);
                countParams.add(ruleAtomicParam);
            }
            // 将封装好sql的count类条件，放入规则总参数ruleParam
            ruleParam.setUserActionCountParams(countParams);

            // 放入seqsql
            ruleParam.setActionSequenceQuerySql(seqSqls);


            // 构建一个queryRouter
            QueryRouterV4 queryRouterV4 = new QueryRouterV4();
            queryRouterV4.setEventState(eventState);


            // 构造DroolFact对象
            // TODO 这里还可以完善： 判断规则的类型（规则组），给它注入对应的queryRouter
//             String className = stateBean.getRouterClass();
//             String classJavaCode = stateBean.getClassCode()
//             // 即时编译api调用
//             Class cls = jitCompile(classJavaCode)
//             QueryRouter router = (QueryRouter)cls.newInstance()
            DroolFact droolFact = new DroolFact(logBean, ruleParam, queryRouterV4, false);

            // 将droolfact插入kiesssion
            kieSession.insert(droolFact);

            // 发射  fire
            kieSession.fireAllRules();

            // 判断计算后的结果是否匹配
            if (droolFact.isMatch()) {
                // 如果匹配，输出一条匹配结果
                out.collect(new ResultBean(ruleName, logBean.getDeviceId(), logBean.getTimeStamp()));
            }

        }
    }


    /**
     * 处理输入的规则操作信息，是canal从mysql中监听到并写入kafka的json数据
     *
     * @param canalJson
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String canalJson, Context ctx, Collector<ResultBean> out) throws Exception {

        BroadcastState<String, RuleStateBean> mapState = ctx.getBroadcastState(StateDescUtil.ruleKieStateDesc);

        // 解析json成对象
        RuleCanalBean ruleCanalBean = JSON.parseObject(canalJson, RuleCanalBean.class);
        log.debug("收到一个规则库的操作,信息为: {}", ruleCanalBean);

        // 分情况处理获取的规则操作信息（新增，更新，删除，停用，启用）
        RuleOperationHandler.handleRuleOper(ruleCanalBean, mapState);

    }
}
