package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class TestGenericUDFCastWithFormat {

  @BeforeClass
  public static void setup() {
    SessionState ss = SessionState.get();
    if (ss == null) {
      ss = SessionState.start(new HiveConf());
    }
    ss.getConf().setBoolVar(HiveConf.ConfVars.HIVE_USE_SQL_DATETIME_FORMAT, true);
  }

  @Test
  public void testDateToStringWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFToString();
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    testCast(udf, inputOI, new DateWritableV2(Date.valueOf("2009-07-30")), "yyyy-mm-dd", "2009-07-30");
    //TODO
  }

  @Test
  public void testStringToDateWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFToDate();
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    testCast(udf, inputOI, "2009-07-30", "yyyy-mm-dd", "2009-07-30");
    //TODO
  }

  @Test
  public void testStringToTimestampWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFTimestamp();
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    testCast(udf, inputOI, "2009-07-30 00:00:00", "yyyy-mm-dd hh:mm:ss", "2009-07-30 00:00:00");
    //TODO
  }

  @Test
  public void testStringToTimestampTZWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFToTimestampLocalTZ();
    ((GenericUDFToTimestampLocalTZ) udf).setTypeInfo(new TimestampLocalTZTypeInfo("America/New_York"));
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    testCast(udf, inputOI, "2009-07-30 00:00:00 America/Los_Angeles", "yyyy-mm-dd hh:mm:ss", "2009-07-30 03:00:00.0 America/New_York");
    //TODO
  }

  @Test
  public void testTimestampToStringWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFToString();
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    testCast(udf, inputOI, new TimestampWritableV2(Timestamp.valueOf("2009-07-30 00:00:00")), "yyyy-mm-dd hh:mm:ss", "2009-07-30 00:00:00");
    //TODO
  }

  @Test
  public void testTimestampTZToStringWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFToString();
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.writableTimestampTZObjectInspector;
    testCast(udf, inputOI,  new TimestampLocalTZWritable(new TimestampTZ()), "yyyy-mm-dd hh:mm:ss", "1969-12-31 16:00:00.0 US/Pacific");
    //TODO
  }

  private void testCast(
      GenericUDF udf, ObjectInspector inputOI, Object input, String format, String output)
      throws HiveException {

    ObjectInspector[] arguments = {inputOI};
    udf.initialize(arguments);

    GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject(input);
    GenericUDF.DeferredObject formatObj = new GenericUDF.DeferredJavaObject(new Text(format));
    GenericUDF.DeferredObject[] args = {valueObj, formatObj};

    assertEquals(udf.getFuncName() + " test with input type " + inputOI.getCategory()
            + " failed ", output, udf.evaluate(args).toString());

    // Try with null args
    GenericUDF.DeferredObject[] nullArgs = { new GenericUDF.DeferredJavaObject(null) };
    assertNull(udf.getFuncName() + " with null arguments failed", udf.evaluate(nullArgs));
  }
}
