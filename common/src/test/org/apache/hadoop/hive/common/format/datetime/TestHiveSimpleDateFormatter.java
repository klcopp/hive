package org.apache.hadoop.hive.common.format.datetime;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.TimeZone;


public class TestHiveSimpleDateFormatter {

  private HiveDateTimeFormatter formatter = new HiveSimpleDateFormatter();

  @Before
  public void setUp() throws WrongFormatterException {
    formatter.setFormatter(new SimpleDateFormat());
    formatter.setPattern("yyyy-MM-dd HH:mm:ss");
    formatter.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
  }

  @Test
  public void testFormat() {
    verifyFormat("2019-01-01 01:01:01");
    verifyFormat("2019-01-01 00:00:00");
    verifyFormat("1960-01-01 23:00:00");
  }

  private void verifyFormat(String s) {
    Timestamp ts = Timestamp.valueOf(s);
    Assert.assertEquals(s, formatter.format(ts));
  }

  @Test
  public void testParse() throws ParseException {
    verifyParse("2019-01-01 01:10:10");
    verifyParse("1960-01-01 23:00:00");

  }

  private void verifyParse(String s) throws ParseException {
    Timestamp ts = Timestamp.valueOf(s);
    Assert.assertEquals(ts, formatter.parse(s));
  }
}
