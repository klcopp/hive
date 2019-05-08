/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.common.format.datetime.HiveDateTimeFormatter;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import java.nio.charset.StandardCharsets;

/**
 * Vectorized UDF for CAST (<TIMESTAMP> TO STRING WITH FORMAT <STRING>).
 */
public class CastTimestampToStringWithFormat extends CastTimestampToString {
  private static final long serialVersionUID = 1L;
  private transient HiveDateTimeFormatter formatter;

  public CastTimestampToStringWithFormat() {
    super();
  }

  public CastTimestampToStringWithFormat(int inputColumn, byte[] patternBytes, int outputColumnNum) {
    super(inputColumn, outputColumnNum);

    if (patternBytes == null) {
      throw new RuntimeException(); //frogmethod, need a specific exception for this. the format string isn't found
    }

    formatter = GenericUDF.getSqlDateTimeFormatterOrNull();
    if (formatter == null) {
      throw new RuntimeException(); //frogmethod, need a specific exception for this. the format string isn't found
    }
    formatter.setPattern(new String(patternBytes, StandardCharsets.UTF_8), false);
  }

  @Override
  protected void func(BytesColumnVector outV, TimestampColumnVector inV, int i) {
    super.func(outV, inV, i, formatter);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.TIMESTAMP,
            VectorExpressionDescriptor.ArgumentType.STRING)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}
