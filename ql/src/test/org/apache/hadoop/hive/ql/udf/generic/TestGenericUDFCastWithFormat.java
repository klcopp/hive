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

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

/**
 * Tests cast udfs GenericUDFToString, GenericUDFToDate, GenericUDFTimestamp,
 * GenericUDFToTimestampLocalTZ with second format argument.
 * E.g. CAST (<TIMESTAMP> AS STRING WITH FORMAT <STRING>)
 */
public class TestGenericUDFCastWithFormat {

  @Test
  public void testDateToStringWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFToString();
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    testCast(udf, inputOI, new DateWritableV2(Date.valueOf("2009-07-30")), "yyyy-MM-dd", "2009-07-30");
    testCast(udf, inputOI, new DateWritableV2(Date.valueOf("2009-07-30")), "yyyy", "2009");
    testCast(udf, inputOI, new DateWritableV2(Date.valueOf("1969-07-30")), "dd", "30");
  }

  @Test
  public void testStringToDateWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFToDate();
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    testCast(udf, inputOI, "2009-07-30", "yyyy-MM-dd", "2009-07-30");
    testCast(udf, inputOI, "2009", "yyyy", "2009-01-01");
    testCast(udf, inputOI, "30", "dd", "1970-01-30");
  }

  @Test
  public void testStringToTimestampWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFTimestamp();
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    testCast(udf, inputOI, "2009-07-30 01:02:03", "yyyy-MM-dd HH24:mi:ss", "2009-07-30 01:02:03");
    testCast(udf, inputOI, "2009", "yyyy", "2009-01-01 00:00:00");
    testCast(udf, inputOI, "07/30/2009 11:0200", "MM/dd/yyyy hh24:miss", "2009-07-30 11:02:00");
    testCast(udf, inputOI, "69.07.30.", "yy.MM.dd.", "1969-07-30 00:00:00");
  }

  @Test
  public void testStringToTimestampTZWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFToTimestampLocalTZ();
    ((GenericUDFToTimestampLocalTZ) udf).setTypeInfo(new TimestampLocalTZTypeInfo("America/Los_Angeles")); //frogmethod probably needs to be local tz.
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    testCast(udf, inputOI, "2009-07-30 07:00:00 America/New_York", "yyyy-MM-dd HH24:mi:ss", "2009-07-30 04:00:00.0 America/Los_Angeles");
    //TODO
  }

  @Test
  public void testTimestampToStringWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFToString();
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    testCast(udf, inputOI, new TimestampWritableV2(Timestamp.valueOf("2009-07-30 00:00:08")), "yyyy-MM-dd HH24:mi:ss", "2009-07-30 00:00:08");
    testCast(udf, inputOI, new TimestampWritableV2(Timestamp.valueOf("2009-07-30 11:02:00")), "MM/dd/yyyy hh24miss", "07/30/2009 110200");
    testCast(udf, inputOI, new TimestampWritableV2(Timestamp.valueOf("2009-07-30 01:02:03")), "MM", "07");
    testCast(udf, inputOI, new TimestampWritableV2(Timestamp.valueOf("1969-07-30 00:00:00")), "yy", "69");
  }

  @Test
  public void testTimestampTZToStringWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFToString();
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.writableTimestampTZObjectInspector;
    testCast(udf, inputOI, new TimestampLocalTZWritable(TimestampTZUtil.parse("2018-01-01 00:00:00 America/New_York")), "yyyy-MM-dd HH24:mi:ss", "2017-12-31 21:00:00");
  }

  private void testCast(
      GenericUDF udf, ObjectInspector inputOI, Object input, String format, String output)
      throws HiveException {

    ConstantObjectInspector formatOI =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
            TypeInfoFactory.getPrimitiveTypeInfo("string"), new Text(format));
    ObjectInspector[] arguments = {inputOI, formatOI};
    udf.initialize(arguments);

    GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject(input);
    GenericUDF.DeferredObject formatObj = new GenericUDF.DeferredJavaObject(new Text(format));
    GenericUDF.DeferredObject[] args = {valueObj, formatObj};

    assertEquals("cast " + inputOI.getTypeName() + " to " + udf.getFuncName() + " failed ",
        output, udf.evaluate(args).toString());

    // Try with null args
    GenericUDF.DeferredObject[] nullArgs = {new GenericUDF.DeferredJavaObject(null)};
    assertNull(udf.getFuncName() + " with NULL arguments failed", udf.evaluate(nullArgs));
  }
}
