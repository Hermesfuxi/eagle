package bigdata.hermesfuxi.eagle.rules.dynamicrules.accumulators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

/**
 * 最大值计算：（支持大于Double.MIN_VALUE的数）
 */
@PublicEvolving
public class BigDecimalMaximum implements SimpleAccumulator<BigDecimal> {

  private static final long serialVersionUID = 1L;

  private BigDecimal max = BigDecimal.valueOf(Double.MIN_VALUE);

  private final BigDecimal limit = BigDecimal.valueOf(Double.MIN_VALUE);

  public BigDecimalMaximum() {}

  public BigDecimalMaximum(BigDecimal value) {
    this.max = value;
  }


  //  计算过程
  @Override
  public void add(BigDecimal value) {
    if (value.compareTo(limit) < 0) {
      throw new IllegalArgumentException(
          "BigDecimalMaximum accumulator only supports values greater than Double.MIN_VALUE");
    }
    this.max = max.max(value);
  }

  @Override
  public BigDecimal getLocalValue() {
    return this.max;
  }

  @Override
  public void merge(Accumulator<BigDecimal, BigDecimal> other) {
    this.max = max.max(other.getLocalValue());
  }

  @Override
  public void resetLocal() {
    this.max = BigDecimal.valueOf(Double.MIN_VALUE);
  }

  @Override
  public BigDecimalMaximum clone() {
    BigDecimalMaximum clone = new BigDecimalMaximum();
    clone.max = this.max;
    return clone;
  }

  // ------------------------------------------------------------------------
  //  Utilities
  // ------------------------------------------------------------------------

  @Override
  public String toString() {
    return "BigDecimal " + this.max;
  }
}
