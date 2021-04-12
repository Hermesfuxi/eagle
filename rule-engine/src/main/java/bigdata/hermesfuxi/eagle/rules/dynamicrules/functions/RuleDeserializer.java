package bigdata.hermesfuxi.eagle.rules.dynamicrules.functions;

import bigdata.hermesfuxi.eagle.rules.pojo.RulePojo;
import bigdata.hermesfuxi.eagle.rules.pojo.RuleParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

@Slf4j
public class RuleDeserializer extends RichFlatMapFunction<String, RulePojo> {

  private RuleParser ruleParser;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    ruleParser = new RuleParser();
  }

  @Override
  public void flatMap(String value, Collector<RulePojo> out) throws Exception {
    log.info("{}", value);
    try {
      RulePojo rule = ruleParser.fromString(value);
      if (rule.getRuleState() != RulePojo.RuleState.CONTROL && rule.getRuleId() == null) {
        throw new NullPointerException("ruleId cannot be null: " + rule.toString());
      }
      out.collect(rule);
    } catch (Exception e) {
      log.warn("Failed parsing rule, dropping it:", e);
    }
  }
}
