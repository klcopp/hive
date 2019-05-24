package org.apache.hadoop.hive.common.format.datetime;

import junit.framework.TestCase;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;

import static org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter.getNumberOfTokenGroups;
import static org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter.parseDate;
import static org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter.parseTimestamp;

public class TestDefaultHiveSqlDateTimeFormatter extends TestCase {
  private static final Timestamp _2019_01_01__02_03_04 = Timestamp.ofEpochMilli(1546308184000L);
  private static final Date _2019_01_01 = Date.ofEpochMilli(1546300800000L);


  private Timestamp timestamp(Timestamp input, int nanos) {
    Timestamp output = (Timestamp) input.clone();
    output.setNanos(nanos);
    return output;
  }

  public void testFormatTimestamp() {
    String s1 = "2019-01-01 02:03:04";
    String s2 = "2019-01-01 02:03:04.44444";
    String s3 = "2019-01-01 02:03:04.444444444";
    assertEquals(s1, DefaultHiveSqlDateTimeFormatter.format(_2019_01_01__02_03_04));
    assertEquals(s2, DefaultHiveSqlDateTimeFormatter.format(timestamp(_2019_01_01__02_03_04, 444440000)));
    assertEquals(s3, DefaultHiveSqlDateTimeFormatter.format(timestamp(_2019_01_01__02_03_04, 444444444)));
  }

  public void testFormatDate() {
    String s1 = "2019-01-01";
    assertEquals(s1, DefaultHiveSqlDateTimeFormatter.format(_2019_01_01));
  }

  public void testParseTimestamp() {
    String s1 = "2019-01-01 02:03:04";
    String s2 = "2019-01-01 02:03:04.000";
    String s3 = "2019-01-01T02:03:04Z";
    String s4 = "2019-01-01 02:03:04.44444";
    String s5 = "2019-01-01T02:03:04.44444Z";
    String s6 = "2019.01.01T02....03:04..44444Z";

    assertEquals(_2019_01_01__02_03_04, parseTimestamp(s1));
    assertEquals(_2019_01_01__02_03_04, parseTimestamp(s2));
    assertEquals(_2019_01_01__02_03_04, parseTimestamp(s3));
    assertEquals(timestamp(_2019_01_01__02_03_04, 444440000), parseTimestamp(s4));
    assertEquals(timestamp(_2019_01_01__02_03_04, 444440000), parseTimestamp(s5));
    assertEquals(timestamp(_2019_01_01__02_03_04, 444440000), parseTimestamp(s6));
  }

  public void testParseDate() {
    String s1 = "2019/01///01";
    String s2 = "19/01///01";
    assertEquals(_2019_01_01, parseDate(s1));
    assertEquals(_2019_01_01, parseDate(s2));
  }

  public void testGetNumberOfTokenGroups() {
    assertEquals(4, getNumberOfTokenGroups("2018..39..7T urkey"));
    assertEquals(8, getNumberOfTokenGroups("2019-01-01T02:03:04Z"));
    assertEquals(3, getNumberOfTokenGroups("2019-01-01GMT"));
    assertEquals(4, getNumberOfTokenGroups("2019-01-01Turkey"));
  }
}
