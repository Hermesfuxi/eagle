package bigdata.hermesfuxi.eagle.rules.dynamicrules.accumulators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

/**
 * 最小值计算：（支持小于Double.MAX_VALUE）
 */
@PublicEvolving
public class BigDecimalMinimum implements SimpleAccumulator<BigDecimal> {

  private static final long serialVersionUID = 1L;

  private BigDecimal min = BigDecimal.valueOf(Double.MAX_VALUE);

  private final BigDecimal limit = BigDecimal.valueOf(Double.MAX_VALUE);

  public BigDecimalMinimum() {}

  public BigDecimalMinimum(BigDecimal value) {
    this.min = value;
  }

  //  计算过程
  @Override
  public void add(BigDecimal value) {
    if (value.compareTo(limit) > 0) {
      throw new IllegalArgumentException(
          "BigDecimalMinimum accumulator only supports values less than Double.MAX_VALUE");
    }
    this.min = min.min(value);
  }

  @Override
  public BigDecimal getLocalValue() {
    return this.min;
  }

  @Override
  public void merge(Accumulator<BigDecimal, BigDecimal> other) {
    this.min = min.min(other.getLocalValue());
  }

  @Override
  public void resetLocal() {
    this.min = BigDecimal.valueOf(Double.MAX_VALUE);
  }

  @Override
  public BigDecimalMinimum clone() {
    BigDecimalMinimum clone = new BigDecimalMinimum();
    clone.min = this.min;
    return clone;
  }

  // ------------------------------------------------------------------------
  //  Utilities
  // ------------------------------------------------------------------------

  @Override
  public String toString() {
    return "BigDecimal " + this.min;
  }
}
