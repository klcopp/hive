package org.apache.hadoop.hive.common.format.datetime;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class TestHiveJavaDateTimeFormatter {
  
  private static DateTimeFormatter DATE_TIME_FORMATTER;
  static {
    DateTimeFormatterBuilder
    builder = new DateTimeFormatterBuilder();
    builder.append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    builder.optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd();
    DATE_TIME_FORMATTER = builder.toFormatter();
  }
  private HiveDateTimeFormatter formatter = new HiveJavaDateTimeFormatter();
  
  
  @Before
  public void setUp() throws WrongFormatterException {
    formatter.setFormatter(DATE_TIME_FORMATTER);
  }


  @Test
  public void testFormat() {
    Timestamp ts = Timestamp.valueOf("2019-01-01 00:00:00.99999");
    Assert.assertEquals("2019-01-01 00:00:00.99999", formatter.format(ts));
  }

  @Test
  public void testParse() throws ParseException {
    String s = "2019-01-01 00:00:00.99999";
    Assert.assertEquals(Timestamp.valueOf("2019-01-01 00:00:00.99999"), formatter.parse(s));
  }

}
