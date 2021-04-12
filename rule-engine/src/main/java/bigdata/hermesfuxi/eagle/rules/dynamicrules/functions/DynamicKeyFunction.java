package bigdata.hermesfuxi.eagle.rules.dynamicrules.functions;

import java.util.Iterator;
import java.util.Map.Entry;

import bigdata.hermesfuxi.eagle.rules.config.Transaction;
import bigdata.hermesfuxi.eagle.rules.engine.RulesEvaluator;
import bigdata.hermesfuxi.eagle.rules.pojo.Keyed;
import bigdata.hermesfuxi.eagle.rules.pojo.KeysExtractor;
import bigdata.hermesfuxi.eagle.rules.pojo.RulePojo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/** Implements dynamic data partitioning based on a set of broadcasted rules. */
@Slf4j
public class DynamicKeyFunction
    extends BroadcastProcessFunction<Transaction, RulePojo, Keyed<Transaction, String, Integer>> {

  private RuleCounterGauge ruleCounterGauge;

  @Override
  public void open(Configuration parameters) {
    ruleCounterGauge = new RuleCounterGauge();
    getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", ruleCounterGauge);
  }

  @Override
  public void processElement(
      Transaction event, ReadOnlyContext ctx, Collector<Keyed<Transaction, String, Integer>> out)
      throws Exception {
    ReadOnlyBroadcastState<Integer, RulePojo> rulesState =
        ctx.getBroadcastState(RulesEvaluator.Descriptors.rulesDescriptor);
    forkEventForEachGroupingKey(event, rulesState, out);
  }

  private void forkEventForEachGroupingKey(
      Transaction event,
      ReadOnlyBroadcastState<Integer, RulePojo> rulesState,
      Collector<Keyed<Transaction, String, Integer>> out)
      throws Exception {
    int ruleCounter = 0;
    for (Entry<Integer, RulePojo> entry : rulesState.immutableEntries()) {
      final RulePojo rule = entry.getValue();
      out.collect(
          new Keyed<>(
              event, KeysExtractor.getKey(rule.getGroupingKeyNames(), event), rule.getRuleId()));
      ruleCounter++;
    }
    ruleCounterGauge.setValue(ruleCounter);
  }

  @Override
  public void processBroadcastElement(
          RulePojo rule, Context ctx, Collector<Keyed<Transaction, String, Integer>> out) throws Exception {
    log.info("{}", rule);
    BroadcastState<Integer, RulePojo> broadcastState =
        ctx.getBroadcastState(RulesEvaluator.Descriptors.rulesDescriptor);
    ProcessingUtils.handleRuleBroadcast(rule, broadcastState);
    if (rule.getRuleState() == RulePojo.RuleState.CONTROL) {
      handleControlCommand(rule.getControlType(), broadcastState);
    }
  }

  private void handleControlCommand(
          RulePojo.ControlType controlType, BroadcastState<Integer, RulePojo> rulesState) throws Exception {
    switch (controlType) {
      case DELETE_RULES_ALL:
        Iterator<Entry<Integer, RulePojo>> entriesIterator = rulesState.iterator();
        while (entriesIterator.hasNext()) {
          Entry<Integer, RulePojo> ruleEntry = entriesIterator.next();
          rulesState.remove(ruleEntry.getKey());
          log.info("Removed Rule {}", ruleEntry.getValue());
        }
        break;
    }
  }

  private static class RuleCounterGauge implements Gauge<Integer> {

    private int value = 0;

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public Integer getValue() {
      return value;
    }
  }
}
