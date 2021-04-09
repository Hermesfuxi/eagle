package bigdata.hermesfuxi.eagle.rules.service.router;

import bigdata.hermesfuxi.eagle.rules.buffer.BufferResult;
import bigdata.hermesfuxi.eagle.rules.buffer.RedisBufferManager;
import bigdata.hermesfuxi.eagle.rules.pojo.AtomicRuleParam;
import bigdata.hermesfuxi.eagle.rules.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rules.pojo.RuleParam;
import bigdata.hermesfuxi.eagle.rules.service.UserProfileQueryService;
import bigdata.hermesfuxi.eagle.rules.service.UserProfileQueryServiceHbaseImpl;
import bigdata.hermesfuxi.eagle.rules.service.offline.OfflineRuleQueryCalculateService;
import bigdata.hermesfuxi.eagle.rules.service.offline.UserActionCountQueryServiceClickhouseImpl;
import bigdata.hermesfuxi.eagle.rules.service.offline.UserActionSequenceQueryServiceClickhouseImpl;
import bigdata.hermesfuxi.eagle.rules.service.realtime.RealTimeRuleQueryCalculateService;
import bigdata.hermesfuxi.eagle.rules.service.realtime.UserActionCountQueryServiceStateImpl;
import bigdata.hermesfuxi.eagle.rules.service.realtime.UserActionSequenceQueryServiceStateImpl;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.state.ListState;

import java.io.IOException;
import java.util.*;

/**
 * @author Hermesfuxi
 * desc: 在V3的基础上添加缓存系统
 */
public class QueryRouterV4 {
    private final UserProfileQueryService userProfileQueryService;

    private final RealTimeRuleQueryCalculateService userActionCountQueryStateService;
    private final RealTimeRuleQueryCalculateService userActionSequenceQueryStateService;

    private final OfflineRuleQueryCalculateService userActionCountQueryClickhouseService;
    private final OfflineRuleQueryCalculateService userActionSequenceQueryClickhouseService;

    // 缓存管理器
    private final RedisBufferManager redisBufferManager;

    ListState<LogBean> eventState;

    public ListState<LogBean> getEventState() {
        return eventState;
    }

    public void setEventState(ListState<LogBean> eventState) {
        this.eventState = eventState;
    }

    public QueryRouterV4() throws Exception {
        // 用户画像查询
        userProfileQueryService = new UserProfileQueryServiceHbaseImpl();

        // 实时计算
        userActionCountQueryStateService = new UserActionCountQueryServiceStateImpl();
        userActionSequenceQueryStateService = new UserActionSequenceQueryServiceStateImpl();

        // 离线计算
        userActionCountQueryClickhouseService = new UserActionCountQueryServiceClickhouseImpl();
        userActionSequenceQueryClickhouseService = new UserActionSequenceQueryServiceClickhouseImpl();

        redisBufferManager = new RedisBufferManager();
    }

    // 控制画像条件查询路由
    public boolean profileQuery(String deviceId, RuleParam ruleParam) throws IOException {
        return userProfileQueryService.judgeProfileCondition(deviceId, ruleParam);
    }

    // 控制count类条件查询路由
    public boolean countConditionQuery(LogBean logBean, Iterable<LogBean> logBeans, RuleParam ruleParam) throws Exception {
        // 计算事件时间的前1小时的整点时间戳，作数据切割
        long splitPoint = DateUtils.addHours(DateUtils.ceiling(new Date(logBean.getTimeStamp()), Calendar.HOUR), -2).getTime();
        String deviceId = logBean.getDeviceId();

        List<AtomicRuleParam> userActionCountParams = ruleParam.getUserActionCountParams();
        List<AtomicRuleParam> cachedUserActionCountParams = new ArrayList<>();
        for (int i = 0; i < userActionCountParams.size(); i++) {
            AtomicRuleParam atomicRuleParam = userActionCountParams.get(i);
            // 拼接bufferKey
            String bufferKey = getBufferKey(deviceId, atomicRuleParam);

            // 从redis中取数据
            BufferResult bufferResult = redisBufferManager.getBufferData(bufferKey, atomicRuleParam.getRangeStart(), atomicRuleParam.getRangeEnd(), atomicRuleParam.getCnts());

            switch (bufferResult.getBufferAvailableLevel()) {

                // 如果是部分有效
                case PARTIAL_AVL:
                    // 则更新条件的窗口起始点
                    atomicRuleParam.setRangeStart(bufferResult.getBufferRangeEnd());
                    // 将缓存value值，放入参数对象的realCnt中
                    atomicRuleParam.setRealCnts(bufferResult.getBufferValue());

                    cachedUserActionCountParams.add(atomicRuleParam);
                    break;

                // 如果是完全有效，没必要再查该条件，需要剔除该条件
                case WHOLE_AVL:
                    break;

                case UN_AVL:
                    cachedUserActionCountParams.add(atomicRuleParam);
                    break;

                default:
                    break;
            }
        }

        ArrayList<AtomicRuleParam> offlineRangeParams = new ArrayList<>();  // 离线条件list
        ArrayList<AtomicRuleParam> realTimeRangeParams = new ArrayList<>();  // 实时条件list
        ArrayList<AtomicRuleParam> crossRangeParams = new ArrayList<>();  // 跨界条件list
        for (AtomicRuleParam userActionCountParam : cachedUserActionCountParams) {
            if (userActionCountParam.getRangeEnd() < splitPoint) {
                // 如果条件起始时间 < 分界点，放入离线条件租
                offlineRangeParams.add(userActionCountParam);
            } else if (userActionCountParam.getRangeStart() >= splitPoint) {
                // 如果条件起始时间 >= 分界点，放入实时条件组
                realTimeRangeParams.add(userActionCountParam);
            } else {
                crossRangeParams.add(userActionCountParam);
            }
        }

        // ---------------- 先实时（速度快，数据小），后离线 ————————————————————
        // 计算实时条件组
        if (realTimeRangeParams.size() > 0) {
            // 将规则总参数对象中的“次数类条件”覆盖成： 实时条件组
            ruleParam.setUserActionCountParams(realTimeRangeParams);
            // 交给stateService对这一组条件进行计算
            boolean realTimeUserActionCountQueryFlag = userActionCountQueryStateService.ruleQueryCalculate(logBeans, ruleParam);
            if (!realTimeUserActionCountQueryFlag) {
                return false;
            }
        }

        // 计算离线条件组
        if (offlineRangeParams.size() > 0) {
            // 将规则总参数对象中的“次数类条件”覆盖成： 离线条件组
            ruleParam.setUserActionCountParams(offlineRangeParams);
            boolean offlineUserActionCountQueryFlag = userActionCountQueryClickhouseService.ruleQueryCalculate(deviceId, ruleParam);
            if (!offlineUserActionCountQueryFlag) {
                return false;
            }
        }

        // 计算跨界条件组
        if (crossRangeParams.size() > 0) {
            for (AtomicRuleParam crossRangeParam : crossRangeParams) {
                // 将规则按界点分割成两部分：实时与离线
                long rangeStart = crossRangeParam.getRangeStart();
                long rangeEnd = crossRangeParam.getRangeEnd();

                crossRangeParam.setRangeStart(splitPoint);
                crossRangeParam.setRangeEnd(rangeEnd);
                List<AtomicRuleParam> realTimeAtomicRuleParams = new ArrayList<>();
                realTimeAtomicRuleParams.add(crossRangeParam);
                // 实时计算
                ruleParam.setUserActionCountParams(realTimeAtomicRuleParams);
                // 交给stateService对这一组条件进行计算
                boolean realTimeUserActionCountQueryFlag = userActionCountQueryStateService.ruleQueryCalculate(logBeans, ruleParam);
                if (!realTimeUserActionCountQueryFlag) {
                    return false;
                }

                crossRangeParam.setRangeStart(rangeStart);
                crossRangeParam.setRangeEnd(splitPoint);
                List<AtomicRuleParam> offlineAtomicRuleParams = new ArrayList<>();
                offlineAtomicRuleParams.add(crossRangeParam);
                // 离线计算
                ruleParam.setUserActionCountParams(offlineAtomicRuleParams);
                boolean offlineUserActionCountQueryFlag = userActionCountQueryClickhouseService.ruleQueryCalculate(deviceId, ruleParam);
                if (!offlineUserActionCountQueryFlag) {
                    return false;
                }
            }
        }
        return true;
    }

    // 控制Sequence类条件查询路由
    public boolean sequenceConditionQuery(LogBean logBean, Iterable<LogBean> logBeans, RuleParam ruleParam) throws Exception {
        // 如果序列条件有内容，才开始计算
        // 取出规则中的序列条件
        List<AtomicRuleParam> userActionSequenceParams = ruleParam.getUserActionSequenceParams();
        if (userActionSequenceParams != null && userActionSequenceParams.size() > 0) {
            // 计算事件时间的前1小时的整点时间戳，作数据切割
            long splitPoint = DateUtils.addHours(DateUtils.ceiling(new Date(logBean.getTimeStamp()), Calendar.HOUR), -2).getTime();

            String deviceId = logBean.getDeviceId();

            AtomicRuleParam atomicRuleParam = userActionSequenceParams.get(0);
            // 取出规则中序列条件的时间窗口起止点
            long rangeStart = atomicRuleParam.getRangeStart();
            long rangeEnd = atomicRuleParam.getRangeEnd();

            // 取出规则中的序列的总步骤数
            int totalSteps = userActionSequenceParams.size();

            // 拼接bufferKey
            String bufferKey = getBufferKey(deviceId, userActionSequenceParams);

            // 从redis中取数据
            BufferResult bufferResult = redisBufferManager.getBufferData(bufferKey, rangeStart, rangeEnd, totalSteps);

            switch (bufferResult.getBufferAvailableLevel()) {

                // 如果是部分有效
                case PARTIAL_AVL:
                    // 则更新条件的窗口起始点
                    atomicRuleParam.setRangeStart(bufferResult.getBufferRangeEnd());

                    // 将缓存value值，放入参数对象的maxStep中
                    ruleParam.setUserActionSequenceQueriedMaxStep(bufferResult.getBufferValue());

                    // 截短条件序列
                    List<AtomicRuleParam> newSequenceList = userActionSequenceParams.subList(bufferResult.getBufferValue(), totalSteps);
                    ruleParam.setUserActionSequenceParams(newSequenceList);
                    break;

                case UN_AVL:
                    break;

                default:
                    return true;
            }
            // 开始分路控制，有如下3中可能性：只查实时/只查离线/跨界查询

            // 只查实时:如果条件的时间窗口起始点>分界点，则在state中查询
            if (rangeStart >= splitPoint) {
                return userActionSequenceQueryStateService.ruleQueryCalculate(logBeans, ruleParam);
            }
            // 只查实时: 如果条件的时间窗口结束点<分界点，则在clickhouse中查询
            else if (rangeEnd < splitPoint) {
                return userActionSequenceQueryClickhouseService.ruleQueryCalculate(deviceId, ruleParam);
            }
            // 跨界查询: 如果条件的时间窗口跨越分界点，则进行双路查询
            else {
                modifyTimeRange(userActionSequenceParams, rangeStart, splitPoint);
                // 执行clickhouse查询
                boolean b1 = userActionSequenceQueryClickhouseService.ruleQueryCalculate(deviceId, ruleParam);
                int farMaxStep = ruleParam.getUserActionSequenceQueriedMaxStep();
                if (b1) {
                    return true;
                }

                // 如果远期部分不足以满足整个条件，则将条件截短
                // 修改时间窗口
                modifyTimeRange(userActionSequenceParams, splitPoint, rangeEnd);
                // 截短条件序列
                ruleParam.setUserActionSequenceParams(userActionSequenceParams.subList(farMaxStep, userActionSequenceParams.size()));
                // 执行state查询
                boolean b2 = userActionSequenceQueryStateService.ruleQueryCalculate(logBeans, ruleParam);
                int nearMaxStep = ruleParam.getUserActionSequenceQueriedMaxStep();

                // 将整合最终结果，塞回参数对象
                ruleParam.setUserActionSequenceQueriedMaxStep(farMaxStep + nearMaxStep);

                return farMaxStep + nearMaxStep >= totalSteps;
            }

        }
        return true;
    }

    private void modifyTimeRange(List<AtomicRuleParam> userActionSequenceParams, long newStart, long newEnd) {
        for (AtomicRuleParam userActionSequenceParam : userActionSequenceParams) {
            userActionSequenceParam.setRangeStart(newStart);
            userActionSequenceParam.setRangeEnd(newEnd);
        }
    }

    private String getBufferKey(String deviceId, AtomicRuleParam atomicParam) {
        // deviceId-EVENT-p1-v1-p2-v2
        StringBuilder sb = new StringBuilder();
        sb.append(deviceId).append("-").append(atomicParam.getEventId());

        HashMap<String, String> properties = atomicParam.getProperties();
        Set<Map.Entry<String, String>> entries = properties.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            sb.append("-").append(entry.getKey()).append("-").append(entry.getValue());
        }

        return sb.toString();
    }

    public static String getBufferKey(String deviceId, List<AtomicRuleParam> paramList) {
        // deviceId-EVENT1-p1-v1-p2-v2-EVENT2-p1-v1-p2-v2
        StringBuilder sb = new StringBuilder();
        sb.append(deviceId);

        for (AtomicRuleParam ruleAtomicParam : paramList) {
            sb.append("-").append(ruleAtomicParam.getEventId());
            HashMap<String, String> properties = ruleAtomicParam.getProperties();
            Set<Map.Entry<String, String>> entries = properties.entrySet();
            for (Map.Entry<String, String> entry : entries) {
                sb.append("-").append(entry.getKey()).append("-").append(entry.getValue());
            }
        }

        return sb.toString();
    }
}
