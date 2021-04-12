package bigdata.hermesfuxi.eagle.rules.dynamicrules.functions;

import java.util.HashSet;
import java.util.Set;

import bigdata.hermesfuxi.eagle.rules.pojo.RulePojo;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;

class ProcessingUtils {

  static void handleRuleBroadcast(RulePojo rule, BroadcastState<Integer, RulePojo> broadcastState)
      throws Exception {
    switch (rule.getRuleState()) {
      case ACTIVE:
      case PAUSE:
        broadcastState.put(rule.getRuleId(), rule);
        break;
      case DELETE:
        broadcastState.remove(rule.getRuleId());
        break;
    }
  }

  static <K, V> Set<V> addToStateValuesSet(MapState<K, Set<V>> mapState, K key, V value)
      throws Exception {

    Set<V> valuesSet = mapState.get(key);

    if (valuesSet != null) {
      valuesSet.add(value);
    } else {
      valuesSet = new HashSet<>();
      valuesSet.add(value);
    }
    mapState.put(key, valuesSet);
    return valuesSet;
  }
}
