package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.common.format.datetime.HiveDateTimeFormatter;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import java.nio.charset.StandardCharsets;

public class CastStringToDateWithFormat extends CastStringToDate {

  private HiveDateTimeFormatter formatter;

  public CastStringToDateWithFormat() {
    super();
  }

  public CastStringToDateWithFormat(int inputColumn, byte[] patternBytes, int outputColumnNum) {
    super(inputColumn, outputColumnNum);

    if (patternBytes == null) {
      throw new RuntimeException(); //frogmethod, need a specific exception for this. the format string isn't found
    }

    formatter = GenericUDF.getSqlDateTimeFormatterOrNull();
    if (formatter == null) {
      throw new RuntimeException(); //frogmethod, need a specific exception for this. the conf is off and you can't use this now
    }

    formatter.setPattern(new String(patternBytes, StandardCharsets.UTF_8));
  }

  @Override
  protected void evaluate(LongColumnVector outputColVector,
      BytesColumnVector inputColVector, int i) {
    super.evaluate(outputColVector, inputColVector, i, formatter);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
            VectorExpressionDescriptor.ArgumentType.STRING)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}
