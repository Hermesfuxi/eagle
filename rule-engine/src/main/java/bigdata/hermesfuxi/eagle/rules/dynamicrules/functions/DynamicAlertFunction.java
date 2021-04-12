package bigdata.hermesfuxi.eagle.rules.dynamicrules.functions;

import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;

import bigdata.hermesfuxi.eagle.rules.config.Transaction;
import bigdata.hermesfuxi.eagle.rules.engine.RulesEvaluator;
import bigdata.hermesfuxi.eagle.rules.pojo.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/** Implements main rule evaluation and alerting logic. */
@Slf4j
public class DynamicAlertFunction
    extends KeyedBroadcastProcessFunction<
        String, Keyed<Transaction, String, Integer>, RulePojo, Alert> {

  private static final String COUNT = "COUNT_FLINK";
  private static final String COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK";

  private static int WIDEST_RULE_KEY = Integer.MIN_VALUE;
  private static int CLEAR_STATE_COMMAND_KEY = Integer.MIN_VALUE + 1;

  private transient MapState<Long, Set<Transaction>> windowState;
  private Meter alertMeter;

  private MapStateDescriptor<Long, Set<Transaction>> windowStateDescriptor =
      new MapStateDescriptor<>(
          "windowState",
          BasicTypeInfo.LONG_TYPE_INFO,
          TypeInformation.of(new TypeHint<Set<Transaction>>() {}));

  @Override
  public void open(Configuration parameters) {

    windowState = getRuntimeContext().getMapState(windowStateDescriptor);

    alertMeter = new MeterView(60);
    getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
  }

  @Override
  public void processElement(
          Keyed<Transaction, String, Integer> value, ReadOnlyContext ctx, Collector<Alert> out)
      throws Exception {

    long currentEventTime = value.getWrapped().getEventTime();

    ProcessingUtils.addToStateValuesSet(windowState, currentEventTime, value.getWrapped());

    long ingestionTime = value.getWrapped().getIngestionTimestamp();
    ctx.output(RulesEvaluator.Descriptors.latencySinkTag, System.currentTimeMillis() - ingestionTime);

    RulePojo rule = ctx.getBroadcastState(RulesEvaluator.Descriptors.rulesDescriptor).get(value.getId());

    if (noRuleAvailable(rule)) {
      log.error("Rule with ID {} does not exist", value.getId());
      return;
    }

    if (rule.getRuleState() == RulePojo.RuleState.ACTIVE) {
      Long windowStartForEvent = rule.getWindowStartFor(currentEventTime);

      long cleanupTime = (currentEventTime / 1000) * 1000;
      ctx.timerService().registerEventTimeTimer(cleanupTime);

      SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
      for (Long stateEventTime : windowState.keys()) {
        if (isStateValueInWindow(stateEventTime, windowStartForEvent, currentEventTime)) {
          aggregateValuesInState(stateEventTime, aggregator, rule);
        }
      }
      BigDecimal aggregateResult = aggregator.getLocalValue();
      boolean ruleResult = rule.apply(aggregateResult);

      ctx.output(
          RulesEvaluator.Descriptors.demoSinkTag,
          "Rule "
              + rule.getRuleId()
              + " | "
              + value.getKey()
              + " : "
              + aggregateResult.toString()
              + " -> "
              + ruleResult);

      if (ruleResult) {
        if (COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
          evictAllStateElements();
        }
        alertMeter.markEvent();
        out.collect(
            new Alert<>(
                rule.getRuleId(), rule, value.getKey(), value.getWrapped(), aggregateResult));
      }
    }
  }

  @Override
  public void processBroadcastElement(RulePojo rule, Context ctx, Collector<Alert> out)
      throws Exception {
    log.info("{}", rule);
    BroadcastState<Integer, RulePojo> broadcastState =
        ctx.getBroadcastState(RulesEvaluator.Descriptors.rulesDescriptor);
    ProcessingUtils.handleRuleBroadcast(rule, broadcastState);
    updateWidestWindowRule(rule, broadcastState);
    if (rule.getRuleState() == RulePojo.RuleState.CONTROL) {
      handleControlCommand(rule, broadcastState, ctx);
    }
  }

  private void handleControlCommand(
          RulePojo command, BroadcastState<Integer, RulePojo> rulesState, Context ctx) throws Exception {
    RulePojo.ControlType controlType = command.getControlType();
    switch (controlType) {
      case EXPORT_RULES_CURRENT:
        for (Entry<Integer, RulePojo> entry : rulesState.entries()) {
          ctx.output(RulesEvaluator.Descriptors.currentRulesSinkTag, entry.getValue());
        }
        break;
      case CLEAR_STATE_ALL:
        ctx.applyToKeyedState(windowStateDescriptor, (key, state) -> state.clear());
        break;
      case CLEAR_STATE_ALL_STOP:
        rulesState.remove(CLEAR_STATE_COMMAND_KEY);
        break;
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

  private boolean isStateValueInWindow(
      Long stateEventTime, Long windowStartForEvent, long currentEventTime) {
    return stateEventTime >= windowStartForEvent && stateEventTime <= currentEventTime;
  }

  private void aggregateValuesInState(
      Long stateEventTime, SimpleAccumulator<BigDecimal> aggregator, RulePojo rule) throws Exception {
    Set<Transaction> inWindow = windowState.get(stateEventTime);
    if (COUNT.equals(rule.getAggregateFieldName())
        || COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
      for (Transaction event : inWindow) {
        aggregator.add(BigDecimal.ONE);
      }
    } else {
      for (Transaction event : inWindow) {
        BigDecimal aggregatedValue =
            FieldsExtractor.getBigDecimalByName(rule.getAggregateFieldName(), event);
        aggregator.add(aggregatedValue);
      }
    }
  }

  private boolean noRuleAvailable(RulePojo rule) {
    // This could happen if the BroadcastState in this CoProcessFunction was updated after it was
    // updated and used in `DynamicKeyFunction`
    if (rule == null) {
      return true;
    }
    return false;
  }

  private void updateWidestWindowRule(RulePojo rule, BroadcastState<Integer, RulePojo> broadcastState)
      throws Exception {
    RulePojo widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);

    if (rule.getRuleState() != RulePojo.RuleState.ACTIVE) {
      return;
    }

    if (widestWindowRule == null) {
      broadcastState.put(WIDEST_RULE_KEY, rule);
      return;
    }

    if (widestWindowRule.getWindowMillis() < rule.getWindowMillis()) {
      broadcastState.put(WIDEST_RULE_KEY, rule);
    }
  }

  @Override
  public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Alert> out)
      throws Exception {

    RulePojo widestWindowRule = ctx.getBroadcastState(RulesEvaluator.Descriptors.rulesDescriptor).get(WIDEST_RULE_KEY);

    Optional<Long> cleanupEventTimeWindow =
        Optional.ofNullable(widestWindowRule).map(RulePojo::getWindowMillis);
    Optional<Long> cleanupEventTimeThreshold =
        cleanupEventTimeWindow.map(window -> timestamp - window);

    cleanupEventTimeThreshold.ifPresent(this::evictAgedElementsFromWindow);
  }

  private void evictAgedElementsFromWindow(Long threshold) {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        Long stateEventTime = keys.next();
        if (stateEventTime < threshold) {
          keys.remove();
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void evictAllStateElements() {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        keys.next();
        keys.remove();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
