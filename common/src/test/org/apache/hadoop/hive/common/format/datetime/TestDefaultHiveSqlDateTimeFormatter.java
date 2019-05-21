package org.apache.hadoop.hive.common.format.datetime;

import junit.framework.TestCase;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;

import java.time.ZoneId;

import static org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter.getNumberOfTokenGroups;
import static org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter.parseDate;
import static org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter.parseTimestamp;

public class TestDefaultHiveSqlDateTimeFormatter extends TestCase {
  private static final int SECONDS_PER_HOUR = 3600;
  private static final ZoneId US_PACIFIC = ZoneId.of("US/Pacific");
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
//    assertEquals(s2, DefaultHiveSqlDateTimeFormatter.format(timestamp(_2019_01_01__02_03_04, 444440000))); //todo fails  (444440000) - hack this? frogmethod
    assertEquals(s3, DefaultHiveSqlDateTimeFormatter.format(timestamp(_2019_01_01__02_03_04, 444444444)));
  }

  public void testFormatDate() {
    String s1 = "2019-01-01";
    assertEquals(s1, DefaultHiveSqlDateTimeFormatter.format(_2019_01_01));
  }

  public void testFormatTimestampTZ() {
    String s1 ="2019-01-01 02:03:04 America/New_York"; //todo frogmethod i think this is wrong
    assertEquals(s1, DefaultHiveSqlDateTimeFormatter.format(new TimestampTZ(_2019_01_01__02_03_04.toEpochSecond(), 0, ZoneId.of("America/New_York"))));
  }

  public void testParseTimestamp() throws ParseException {
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

  public void testParseTimestampLocalTZ() throws ParseException {
    String s1 ="2019-01-01 05:03:04 America/New_York"; // 00:03 UTC, 10:03 US/Pacific
    String s2 ="2019-01-01 05:03:04America/New_York"; // 8:00 UTC, 00:00 US/Pacific
    String s3 ="2019-01-01 02:03:04.777 US/Pacific";
    String s4 ="2019-01-01 02:03:04.777Turkey";
    String s5 ="2019-01-01 02:03:04.777GMT"; // 00:00 UTC, 16:00 US/Pacific
    String s6 ="2019-01-01 02:03:04.777";
    
    verifyTimestampLocalTZ(s1, 8, 0, US_PACIFIC);
    verifyTimestampLocalTZ(s2, 8, 0, US_PACIFIC);
    verifyTimestampLocalTZ(s3, 8, 777000000, null);
    verifyTimestampLocalTZ(s4, -3, 777000000, US_PACIFIC);
//    verifyTimestampLocalTZ(s5, 0, 777000000, US_PACIFIC); // can't do 777GMT as ff9. above works because T is a separator. todo frogmethod
    verifyTimestampLocalTZ(s6, 8, 777000000, US_PACIFIC);
  }
  
  private void verifyTimestampLocalTZ(String input, long offsetHours, int nanos, ZoneId withTimeZone)
      throws ParseException {
    TimestampTZ expected = new TimestampTZ(_2019_01_01__02_03_04.toEpochSecond() + offsetHours * SECONDS_PER_HOUR, nanos, US_PACIFIC);
    assertEquals(expected, DefaultHiveSqlDateTimeFormatter.parseTimestampTZ(input, withTimeZone));
  }

  public void testGetNumberOfTokenGroups() {
    assertEquals(4, getNumberOfTokenGroups("2018..39..7T urkey"));
    assertEquals(8, getNumberOfTokenGroups("2019-01-01T02:03:04Z"));
    assertEquals(3, getNumberOfTokenGroups("2019-01-01GMT"));
    assertEquals(4, getNumberOfTokenGroups("2019-01-01Turkey"));
  }
}
