package bigdata.hermesfuxi.eagle.rules.pojo;

public class Alert<Event, Value> {
  private Integer ruleId;
  private RulePojo violatedRule;
  private String key;

  private Event triggeringEvent;
  private Value triggeringValue;

  public Alert() {
  }

  public Alert(Integer ruleId, RulePojo violatedRule, String key, Event triggeringEvent, Value triggeringValue) {
    this.ruleId = ruleId;
    this.violatedRule = violatedRule;
    this.key = key;
    this.triggeringEvent = triggeringEvent;
    this.triggeringValue = triggeringValue;
  }

  public Integer getRuleId() {
    return ruleId;
  }

  public void setRuleId(Integer ruleId) {
    this.ruleId = ruleId;
  }

  public RulePojo getViolatedRule() {
    return violatedRule;
  }

  public void setViolatedRule(RulePojo violatedRule) {
    this.violatedRule = violatedRule;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Event getTriggeringEvent() {
    return triggeringEvent;
  }

  public void setTriggeringEvent(Event triggeringEvent) {
    this.triggeringEvent = triggeringEvent;
  }

  public Value getTriggeringValue() {
    return triggeringValue;
  }

  public void setTriggeringValue(Value triggeringValue) {
    this.triggeringValue = triggeringValue;
  }
}
