package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.fail;

public class TestGenericUDFCastWithFormat {

  @Test
  public void testStringToDateWithFormat() throws HiveException {
    GenericUDF udf = new GenericUDFToDate();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    SessionState ss = SessionState.get();
    if (ss != null) {
      ss.getConf().setBoolVar(HiveConf.ConfVars.HIVE_USE_SQL_DATETIME_FORMAT, true);
    } else {
      fail();
    }
    udf.initialize(arguments);
    GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject(new Text("2009-07-30"));
    GenericUDF.DeferredObject formatObj = new GenericUDF.DeferredJavaObject(new Text("yyyy-mm-dd"));
    GenericUDF.DeferredObject[] args = {valueObj, formatObj};
    DateWritableV2 output = (DateWritableV2) udf.evaluate(args);

    assertEquals("to_date() test for STRING failed ", "2009-07-30", output.toString());

    // Try with null args
    GenericUDF.DeferredObject[] nullArgs = { new GenericUDF.DeferredJavaObject(null) };
    output = (DateWritableV2) udf.evaluate(nullArgs);
    assertNull("to_date() with null STRING", output);
  }
}
