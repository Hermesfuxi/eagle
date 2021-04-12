package bigdata.hermesfuxi.eagle.rules.pojo;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

public class RuleParser {

  public RulePojo fromString(String line) throws IOException {
    if (line.length() > 0 && '{' == line.charAt(0)) {
      return JSONObject.parseObject(line, RulePojo.class);
    } else {
      return parsePlain(line);
    }
  }

  private static RulePojo parsePlain(String ruleString) throws IOException {
    List<String> tokens = Arrays.asList(ruleString.split(","));
    if (tokens.size() != 9) {
      throw new IOException("Invalid rule (wrong number of tokens): " + ruleString);
    }

    Iterator<String> iter = tokens.iterator();
    RulePojo rule = new RulePojo();

    rule.setRuleId(Integer.parseInt(stripBrackets(iter.next())));
    rule.setRuleState(RulePojo.RuleState.valueOf(stripBrackets(iter.next()).toUpperCase()));
    rule.setGroupingKeyNames(getNames(iter.next()));
    rule.setUnique(getNames(iter.next()));
    rule.setAggregateFieldName(stripBrackets(iter.next()));
    rule.setAggregatorFunctionType(
        RulePojo.AggregatorFunctionType.valueOf(stripBrackets(iter.next()).toUpperCase()));
    rule.setLimitOperatorType(RulePojo.LimitOperatorType.fromString(stripBrackets(iter.next())));
    rule.setLimit(new BigDecimal(stripBrackets(iter.next())));
    rule.setWindowMinutes(Integer.parseInt(stripBrackets(iter.next())));

    return rule;
  }

  private static String stripBrackets(String expression) {
    return expression.replaceAll("[()]", "");
  }

  private static List<String> getNames(String expression) {
    String keyNamesString = expression.replaceAll("[()]", "");
    if (!"".equals(keyNamesString)) {
      String[] tokens = keyNamesString.split("&", -1);
      return Arrays.asList(tokens);
    } else {
      return new ArrayList<>();
    }
  }
}
