package bigdata.hermesfuxi.eagle.rules.pojo;

import java.io.Serializable;
import java.util.List;

/**
 * @author hermesfuxi
 * desc 规则整体条件封装成抽象实体：可视为一个节点(下面四个条件属于通常只会有一个有值)
 * TODO 与、或、非逻辑
 * 完善的规则引擎，必然如同 sql、正则表达式、搜索引擎语法一般
 * 数据结构：当前使用的是树型结构，如同DOM树，根节点-父节点与子节点
 */
public class RuleParamNode implements Serializable {
    // 规则组ID：必须有值，如同 dom树中的节点ID
    private String ruleParamNodeId;

    //条件分类：简单条件、组合条件、迭代条件

    // 当前节点的原子条件（不可差分，如 a = b , a != b）：自带 与、或、非逻辑
    private AtomicRuleParam atomicRuleParam;

    // 当前节点的子节点：
    // 类型分无序与有序
    private String ruleParamGroupType;
    // 无序模式
    // 由子条件形成的条件组: 与逻辑
    private List<RuleParamNode> ruleParamChildrenNodeGroup;
    // 由子条件形成的条件组: 或逻辑
    private List<RuleParamNode> orRuleParamGroup;
    // 由子条件形成的条件组: 非逻辑
    private List<RuleParamNode> notRuleParamGroup;

    // 当前节点的子节点： 有序模式-整个条件队列的模式序列
    // 不同的“近邻”模式: 严格近邻/宽松近邻/非确定性宽松近邻
    // 严格非近邻/宽松非近邻/非确定性宽松非近邻


    // 计算的中间结果：用于缓存
    // 执行的事件流索引
    private long eventIndex;
    // 执行最大步长
    private long maxStep;

    // 次数统计(比如1分钟内某账号的登录次数，可以用来分析盗号等)
    private long accumulator;


    // 统计学：抽象：某时间段，在条件维度（可以是多个维度复合）下，利用统计方法统计结果维度的值。
    // 次频统计(比如连续三次密码输错，可以用来分析盗号等)

    // 时频统计（比如1小时内某ip上出现的账号，可以用来分析黄牛党等）

    // 最大统计（比如用户交易金额比历史交易都大，可能有风险）

    // 最近统计（比如最近一次交易才过数秒，可能机器下单）

    // 行为习惯（比如用户常用登录地址，用户经常登录时间段，可以用来分析盗号等）

    // 阈值约束；最小最大值（开闭）


    // 时间约束：起止时间区间（开闭）
    // 时间语义：事件时间、系统时间
    // 周期时间（定时器）

}
