/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampLocalTZConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Convert from string to TIMESTAMP WITH LOCAL TIME ZONE.
 */
@Description(name = "timestamp with local time zone",
    value = "CAST(<STRING> as TIMESTAMP WITH LOCAL TIME ZONE [FORMAT <STRING>]) - returns the" +
        "timestamp with local time zone represented by string. Optional parsing according to " +
        "format string.",
    extended = "The string should be of format 'yyyy-MM-dd HH:mm:ss[.SSS...] ZoneId/ZoneOffset'," 
        + "Examples of ZoneId and ZoneOffset are Asia/Shanghai and GMT+08:00. " 
        + "The time and zone parts are optional. If time is absent, '00:00:00.0' will be used." 
        + "If zone is absent, the system time zone will be used.\n"
        + "If format is specified with FORMAT argument then SQL:2016 datetime formats will be " 
        + "used. hive.use.sql.datetime.formats must be turned on to use formats.")

public class GenericUDFToTimestampLocalTZ extends GenericUDF implements SettableUDF {

  private transient PrimitiveObjectInspector argumentOI;
  private transient PrimitiveObjectInspectorConverter.TimestampLocalTZConverter converter;

  private TimestampLocalTZTypeInfo typeInfo;
  private HiveDateTimeFormatter formatter = null;
  private boolean useSql;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function CAST as TIMESTAMP WITH LOCAL TIME ZONE requires at least one argument, got "
              + arguments.length);
    }
    try {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
      switch (argumentOI.getPrimitiveCategory()) {
      case CHAR:
      case VARCHAR:
      case STRING:
      case DATE:
      case TIMESTAMP:
      case TIMESTAMPLOCALTZ:
        break;
      default:
        throw new UDFArgumentException("CAST as TIMESTAMP WITH LOCAL TIME ZONE only allows" +
            "string/date/timestamp/timestamp with time zone types");
      }
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function CAST as TIMESTAMP WITH LOCAL TIME ZONE takes only primitive types");
    }
    SettableTimestampLocalTZObjectInspector outputOI = (SettableTimestampLocalTZObjectInspector)
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);
    converter = new TimestampLocalTZConverter(argumentOI, outputOI);

    // for CAST WITH FORMAT
    if (arguments.length > 1 && arguments[1] != null && (useSql || useSqlFormat())) {
      formatter = new HiveSqlDateTimeFormatter();
      formatter.setPattern(getConstantStringValue(arguments, 1));
      converter.setDateTimeFormatter(formatter);
    }

    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }

    return converter.convert(o0);
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1 || children.length == 2);
    StringBuilder sb = new StringBuilder();
    sb.append("CAST( ");
    sb.append(children[0]);
    sb.append(" AS ");
    sb.append(typeInfo.getTypeName());
    if (children.length == 2) {
      sb.append(" FORMAT ");
      sb.append(children[1]);
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public TypeInfo getTypeInfo() {
    return typeInfo;
  }

  @Override
  public void setTypeInfo(TypeInfo typeInfo) throws UDFArgumentException {
    this.typeInfo = (TimestampLocalTZTypeInfo) typeInfo;
  }

  /**
   * Get whether or not to use Sql formats.
   * Necessary because MapReduce tasks don't have access to SessionState conf, so need to use
   * MapredContext conf. This is only called in runtime of MapRedTask.
   */
  @Override public void configure(MapredContext context) {
    super.configure(context);
    useSql =
        HiveConf.getBoolVar(context.getJobConf(), HiveConf.ConfVars.HIVE_USE_SQL_DATETIME_FORMAT);
  }
}
