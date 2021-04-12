package bigdata.hermesfuxi.eagle.rules.pojo;

import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.api.common.time.Time;

public class RulePojo {

  private Integer ruleId;
  private RuleState ruleState;
  private List<String> groupingKeyNames; // aggregation
  private List<String> unique;
  private String aggregateFieldName;
  private AggregatorFunctionType aggregatorFunctionType;
  private LimitOperatorType limitOperatorType;
  private BigDecimal limit;
  private Integer windowMinutes;
  private ControlType controlType;

  public RulePojo() {
  }

  public RulePojo(Integer ruleId, RuleState ruleState, List<String> groupingKeyNames, List<String> unique, String aggregateFieldName, AggregatorFunctionType aggregatorFunctionType, LimitOperatorType limitOperatorType, BigDecimal limit, Integer windowMinutes, ControlType controlType) {
    this.ruleId = ruleId;
    this.ruleState = ruleState;
    this.groupingKeyNames = groupingKeyNames;
    this.unique = unique;
    this.aggregateFieldName = aggregateFieldName;
    this.aggregatorFunctionType = aggregatorFunctionType;
    this.limitOperatorType = limitOperatorType;
    this.limit = limit;
    this.windowMinutes = windowMinutes;
    this.controlType = controlType;
  }

  public Long getWindowMillis() {
    return Time.minutes(this.windowMinutes).toMilliseconds();
  }

  /**
   * Evaluates this rule by comparing provided value with rules' limit based on limit operator type.
   *
   * @param comparisonValue value to be compared with the limit
   */
  public boolean apply(BigDecimal comparisonValue) {
    switch (limitOperatorType) {
      case EQUAL:
        return comparisonValue.compareTo(limit) == 0;
      case NOT_EQUAL:
        return comparisonValue.compareTo(limit) != 0;
      case GREATER:
        return comparisonValue.compareTo(limit) > 0;
      case LESS:
        return comparisonValue.compareTo(limit) < 0;
      case LESS_EQUAL:
        return comparisonValue.compareTo(limit) <= 0;
      case GREATER_EQUAL:
        return comparisonValue.compareTo(limit) >= 0;
      default:
        throw new RuntimeException("Unknown limit operator type: " + limitOperatorType);
    }
  }

  public long getWindowStartFor(Long timestamp) {
    Long ruleWindowMillis = getWindowMillis();
    return (timestamp - ruleWindowMillis);
  }

  public enum AggregatorFunctionType {
    SUM,
    AVG,
    MIN,
    MAX
  }

  public enum LimitOperatorType {
    EQUAL("="),
    NOT_EQUAL("!="),
    GREATER_EQUAL(">="),
    LESS_EQUAL("<="),
    GREATER(">"),
    LESS("<");

    String operator;

    LimitOperatorType(String operator) {
      this.operator = operator;
    }

    public static LimitOperatorType fromString(String text) {
      for (LimitOperatorType b : LimitOperatorType.values()) {
        if (b.operator.equals(text)) {
          return b;
        }
      }
      return null;
    }
  }

  public enum RuleState {
    ACTIVE,
    PAUSE,
    DELETE,
    CONTROL
  }

  public enum ControlType {
    CLEAR_STATE_ALL,
    CLEAR_STATE_ALL_STOP,
    DELETE_RULES_ALL,
    EXPORT_RULES_CURRENT
  }

  public Integer getRuleId() {
    return ruleId;
  }

  public void setRuleId(Integer ruleId) {
    this.ruleId = ruleId;
  }

  public RuleState getRuleState() {
    return ruleState;
  }

  public void setRuleState(RuleState ruleState) {
    this.ruleState = ruleState;
  }

  public List<String> getGroupingKeyNames() {
    return groupingKeyNames;
  }

  public void setGroupingKeyNames(List<String> groupingKeyNames) {
    this.groupingKeyNames = groupingKeyNames;
  }

  public List<String> getUnique() {
    return unique;
  }

  public void setUnique(List<String> unique) {
    this.unique = unique;
  }

  public String getAggregateFieldName() {
    return aggregateFieldName;
  }

  public void setAggregateFieldName(String aggregateFieldName) {
    this.aggregateFieldName = aggregateFieldName;
  }

  public AggregatorFunctionType getAggregatorFunctionType() {
    return aggregatorFunctionType;
  }

  public void setAggregatorFunctionType(AggregatorFunctionType aggregatorFunctionType) {
    this.aggregatorFunctionType = aggregatorFunctionType;
  }

  public LimitOperatorType getLimitOperatorType() {
    return limitOperatorType;
  }

  public void setLimitOperatorType(LimitOperatorType limitOperatorType) {
    this.limitOperatorType = limitOperatorType;
  }

  public BigDecimal getLimit() {
    return limit;
  }

  public void setLimit(BigDecimal limit) {
    this.limit = limit;
  }

  public Integer getWindowMinutes() {
    return windowMinutes;
  }

  public void setWindowMinutes(Integer windowMinutes) {
    this.windowMinutes = windowMinutes;
  }

  public ControlType getControlType() {
    return controlType;
  }

  public void setControlType(ControlType controlType) {
    this.controlType = controlType;
  }

  @Override
  public String toString() {
    return "Rule{" +
            "ruleId=" + ruleId +
            ", ruleState=" + ruleState +
            ", groupingKeyNames=" + groupingKeyNames +
            ", unique=" + unique +
            ", aggregateFieldName='" + aggregateFieldName + '\'' +
            ", aggregatorFunctionType=" + aggregatorFunctionType +
            ", limitOperatorType=" + limitOperatorType +
            ", limit=" + limit +
            ", windowMinutes=" + windowMinutes +
            ", controlType=" + controlType +
            '}';
  }
}
