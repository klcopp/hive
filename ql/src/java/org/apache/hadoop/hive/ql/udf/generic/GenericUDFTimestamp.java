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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.format.datetime.HiveDateTimeFormatter;
import org.apache.hadoop.hive.common.format.datetime.HiveSqlDateTimeFormatter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToTimestampWithFormat;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDateToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDoubleToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastLongToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToTimestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 *
 * GenericUDFTimestamp
 *
 * Example usage:
 * ... CAST(&lt;Timestamp string&gt; as TIMESTAMP) ...
 *
 * Creates a TimestampWritableV2 object using PrimitiveObjectInspectorConverter
 *
 */
@Description(name = "timestamp",
    value = "cast(<primitive> as timestamp [format <string>]) - Returns timestamp",
    extended = "If format is specified with FORMAT argument then SQL:2016 datetime formats will be "
    + "used. hive.use.sql.datetime.formats must be turned on to use formats.")
@VectorizedExpressions({CastLongToTimestamp.class, CastDateToTimestamp.class,
    CastDoubleToTimestamp.class, CastDecimalToTimestamp.class, CastStringToTimestamp.class,
    CastStringToTimestampWithFormat.class})
public class GenericUDFTimestamp extends GenericUDF {

  private transient PrimitiveObjectInspector argumentOI;
  private transient TimestampConverter tc;
  private HiveDateTimeFormatter formatter = null;
  private boolean useSql;
  /*
   * Integer value was interpreted to timestamp inconsistently in milliseconds comparing
   * to float/double in seconds. Since the issue exists for a long time and some users may
   * use in such inconsistent way, use the following flag to keep backward compatible.
   * If the flag is set to false, integer value is interpreted as timestamp in milliseconds;
   * otherwise, it's interpreted as timestamp in seconds.
   */
  private boolean intToTimestampInSeconds = false;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function TIMESTAMP requires at least one argument, got "
          + arguments.length);
    }

    SessionState ss = SessionState.get();
    if (ss != null) {
      intToTimestampInSeconds = ss.getConf().getBoolVar(ConfVars.HIVE_INT_TIMESTAMP_CONVERSION_IN_SECONDS);
    }

    try {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function TIMESTAMP takes only primitive types");
    }

    tc = new TimestampConverter(argumentOI,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
    tc.setIntToTimestampInSeconds(intToTimestampInSeconds);

    // for CAST WITH FORMAT
    if (arguments.length > 1 && arguments[1] != null && (useSql || useSqlFormat())) {
      formatter = new HiveSqlDateTimeFormatter();
      formatter.setPattern(getConstantStringValue(arguments, 1), true);
      tc.setDateTimeFormatter(formatter);
    }

    return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }
    return tc.convert(o0);
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (1 <= children.length && children.length <= 2);
    StringBuilder sb = new StringBuilder();
    sb.append("CAST( ");
    sb.append(children[0]);
    sb.append(" AS TIMESTAMP");
    if (children.length == 2) {
      sb.append(" FORMAT ");
      sb.append(children[1]);
    }
    sb.append(")");
    return sb.toString();
  }

  public boolean isIntToTimestampInSeconds() {
    return intToTimestampInSeconds;
  }

  /**
   * Get whether or not to use Sql formats.
   * Necessary because MapReduce tasks don't have access to SessionState conf, so need to use
   * MapredContext conf. This is only called in runtime of MapRedTask.
   */
  @Override public void configure(MapredContext context) {
    super.configure(context);
    useSql =
        HiveConf.getBoolVar(context.getJobConf(), ConfVars.HIVE_USE_SQL_DATETIME_FORMAT);
  }
}
