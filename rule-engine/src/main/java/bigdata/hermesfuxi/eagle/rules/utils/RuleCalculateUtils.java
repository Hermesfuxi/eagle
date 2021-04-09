package bigdata.hermesfuxi.eagle.rules.utils;

import bigdata.hermesfuxi.eagle.rules.pojo.LogBean;
import bigdata.hermesfuxi.eagle.rules.pojo.AtomicRuleParam;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Hermesfuxi
 */
public class RuleCalculateUtils {
    /**
     * 工具方法，用于判断一个待判断事件和一个规则中的原子条件是否一致
     */
    public static boolean eventBeanMatchEventParam(LogBean eventBean, AtomicRuleParam eventParam) {
        // 如果传入的一个事件的事件id与参数中的事件id相同，才开始进行属性判断
        if (eventBean.getEventId().equals(eventParam.getEventId())) {

            // 取出待判断事件中的属性
            Map<String, String> eventProperties = eventBean.getProperties();

            // 取出条件中的事件属性
            HashMap<String, String> paramProperties = eventParam.getProperties();
            Set<Map.Entry<String, String>> entries = paramProperties.entrySet();
            // 遍历条件中的每个属性及值
            for (Map.Entry<String, String> entry : entries) {
                if (!entry.getValue().equals(eventProperties.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static boolean eventBeanMatchEventParam(LogBean eventBean, AtomicRuleParam eventParam, boolean neeTimeCompare) {
        boolean flag = eventBeanMatchEventParam(eventBean, eventParam);
        // 要考虑一点，外部传入的条件中，时间范围条件，如果起始、结束没有约束，应该传入一个 -1
        long start = eventParam.getRangeStart();
        long end = eventParam.getRangeEnd();
        long timeStamp = eventBean.getTimeStamp();

        return flag && timeStamp >= (start == -1 ? 0 : start) && timeStamp <= (end == -1 ? Long.MAX_VALUE : end);

    }

}
