package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.TestGenericUDFCastWithFormat;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class TestVectorTypeCastsWithFormat {

  @BeforeClass
  public static void setup() {
    //set hive.use.sql.datetime.formats to true
    TestGenericUDFCastWithFormat.turnOnSqlDateTimeFormats();
  }

  @Test
  public void testCastDateToStringWithFormat() throws HiveException {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchDateInStringOutFormatted();
    BytesColumnVector resultV = (BytesColumnVector) b.cols[1];
    VectorExpression expr = new CastDateToStringWithFormat(0, "yyyy".getBytes(), 1);
    expr.evaluate(b);
    verifyString(0, "2019", resultV);
    verifyString(1, "1776", resultV);
    verifyString(2, "2012", resultV);
    verifyString(3, "1580", resultV);
    verifyString(4, "0005", resultV);
    verifyString(5, "9999", resultV);

    expr = new CastDateToStringWithFormat(0, "MM".getBytes(), 1);
    b.cols[1] = resultV = new BytesColumnVector();
    expr.evaluate(b);
    verifyString(0, "12", resultV);
    verifyString(1, "07", resultV);
    verifyString(2, "02", resultV);
    verifyString(3, "07", resultV); //frogmethod change to 08 when simpledatetime is removed
    verifyString(4, "01", resultV);
    verifyString(5, "12", resultV);
  }

  @Test
  public void testCastTimestampToStringWithFormat() throws HiveException {
    VectorizedRowBatch b =
        TestVectorMathFunctions.getVectorizedRowBatchTimestampInStringOutFormatted();
    BytesColumnVector resultV = (BytesColumnVector) b.cols[1];
    VectorExpression expr = new CastTimestampToStringWithFormat(0, "yyyy".getBytes(), 1);
    expr.evaluate(b);

    Assert.assertEquals("2019", getStringFromBytesColumnVector(resultV, 0));
    Assert.assertEquals("1776", getStringFromBytesColumnVector(resultV, 1));
    Assert.assertEquals("2012", getStringFromBytesColumnVector(resultV, 2));
    Assert.assertEquals("1580", getStringFromBytesColumnVector(resultV, 3));
    Assert.assertEquals("0005", getStringFromBytesColumnVector(resultV, 4));
    Assert.assertEquals("9999", getStringFromBytesColumnVector(resultV, 5));

    b.cols[1] = resultV = new BytesColumnVector();
    expr = new CastTimestampToStringWithFormat(0, "HH".getBytes(), 1);
    expr.evaluate(b);

    Assert.assertEquals("19", getStringFromBytesColumnVector(resultV, 0));
    Assert.assertEquals("17", getStringFromBytesColumnVector(resultV, 1));
    Assert.assertEquals("23", getStringFromBytesColumnVector(resultV, 2));
    Assert.assertEquals("00", getStringFromBytesColumnVector(resultV, 3));
    Assert.assertEquals("00", getStringFromBytesColumnVector(resultV, 4));
    Assert.assertEquals("23", getStringFromBytesColumnVector(resultV, 5));

    //todo frogmethod test nanos (FFFFFFFFF)
  }

  @Test
  public void testCastStringToTimestampWithFormat() throws HiveException {
    VectorizedRowBatch b =
        TestVectorMathFunctions.getVectorizedRowBatchStringInDateTimeOutFormatted();
    TimestampColumnVector resultV;
    b.cols[1] = resultV = new TimestampColumnVector();
    VectorExpression expr = new CastStringToTimestampWithFormat(0, "yyyy".getBytes(), 1);
    expr.evaluate(b);

    verifyTimestamp("2019-01-01 00:00:00", resultV);
    verifyTimestamp("1776-01-01 00:00:00", resultV);
    verifyTimestamp("2012-01-01 00:00:00", resultV);
    verifyTimestamp("1508-01-01 00:00:00", resultV);
    verifyTimestamp("0005-01-01 00:00:00", resultV);
    verifyTimestamp("9999-01-01 00:00:00", resultV);

    b.cols[1] = resultV = new TimestampColumnVector();
    expr = new CastStringToTimestampWithFormat(0, "HH".getBytes(), 1);
    expr.evaluate(b);

    verifyTimestamp("1970-01-01 19:00:00", resultV);
    verifyTimestamp("1970-01-01 17:00:00", resultV);
    verifyTimestamp("1970-01-01 23:00:00", resultV);
    verifyTimestamp("1970-01-01 00:00:00", resultV);
    verifyTimestamp("1970-01-01 00:00:00", resultV);
    verifyTimestamp("1970-01-01 23:00:00", resultV);
    
    //todo frogmethod test nanos (FFFFFFFFF)
  }

  private void verifyTimestamp(String tsString, TimestampColumnVector resultV) {
    Assert.assertEquals(Timestamp.valueOf(tsString).toEpochMilli(), resultV.time[0]);
    Assert.assertEquals(Timestamp.valueOf(tsString).getNanos(), resultV.nanos[0]);
  }

  @Test
  public void testCastStringToDateWithFormat() throws HiveException {
    VectorizedRowBatch b =
        TestVectorMathFunctions.getVectorizedRowBatchStringInDateTimeOutFormatted();
    LongColumnVector resultV;
    b.cols[1] = resultV = new LongColumnVector();
    VectorExpression expr = new CastStringToDateWithFormat(0, "yyyy".getBytes(), 1);
    expr.evaluate(b);

    Assert.assertEquals(Date.valueOf("2019-12-31").toEpochDay(), resultV.vector[0]);
    Assert.assertEquals(Date.valueOf("1776-07-04").toEpochDay(), resultV.vector[0]);
    Assert.assertEquals(Date.valueOf("2012-02-29").toEpochDay(), resultV.vector[0]);
    Assert.assertEquals(Date.valueOf("1580-08-08").toEpochDay(), resultV.vector[0]);
    Assert.assertEquals(Date.valueOf("0005-01-01").toEpochDay(), resultV.vector[0]);
    Assert.assertEquals(Date.valueOf("9999-12-31").toEpochDay(), resultV.vector[0]);
  }

  private void verifyString(int resultIndex, String expected, BytesColumnVector resultV) {
    String result = getStringFromBytesColumnVector(resultV, resultIndex);
    Assert.assertEquals(expected, result);
  }

  private String getStringFromBytesColumnVector(BytesColumnVector resultV, int i) {
    String result;
    byte[] resultBytes = Arrays.copyOfRange(resultV.vector[i], resultV.start[i],
        resultV.start[i] + resultV.length[i]);
    result = new String(resultBytes, StandardCharsets.UTF_8);
    return result;
  }
}
