package org.apache.hadoop.hive.common.format.datetime;

import junit.framework.TestCase;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;

import java.time.ZoneId;
import java.util.TimeZone;

public class TestDefaultHiveSqlDateTimeFormatter extends TestCase {
  public static final int SECONDS_PER_HOUR = 3600;

  //todo Timestamp.valueOf() and Date.valueOf() are going to be redundant here.

  public void testFormatTimestamp() {
    String s1 = "2019-01-01 02:03:04.44444";
    String s2 = "2019-01-01 02:03:04";
    assertEquals(s1, DefaultHiveSqlDateTimeFormatter.format(Timestamp.valueOf(s1))); //todo fails  (444440000) - hack this?
    assertEquals(s2, DefaultHiveSqlDateTimeFormatter.format(Timestamp.valueOf(s2)));
  }

  public void testFormatDate() {
    String s1 = "2019-01-01";
    assertEquals(s1, DefaultHiveSqlDateTimeFormatter.format(Date.valueOf(s1)));
  }

  public void testFormatTimestampTZ() {
    String s1 ="1970-01-01 00:00:00 America/New_York";
    assertEquals(s1, DefaultHiveSqlDateTimeFormatter.format(new TimestampTZ(0, 0, ZoneId.of("America/New_York"))));
  }

  public void testParseTimestamp() throws ParseException {
    String s1 = "2019-01-01 02:03:04.44444";
    String s2 = "2019-01-01 02:03:04";
    String s3 = "2019-01-01 02:03:04.000";
    String s4 = "2019-01-01T02:03:04Z";
    String s5 = "2019-01-01T02:03:04.44444Z";
    String s6 = "2019.01.01T02....03:04..44444Z";

    assertEquals(Timestamp.valueOf(s1), DefaultHiveSqlDateTimeFormatter.parseTimestamp(s1));
    assertEquals(Timestamp.valueOf(s2), DefaultHiveSqlDateTimeFormatter.parseTimestamp(s2));
    assertEquals(Timestamp.valueOf(s2), DefaultHiveSqlDateTimeFormatter.parseTimestamp(s3));
    assertEquals(Timestamp.valueOf(s2), DefaultHiveSqlDateTimeFormatter.parseTimestamp(s4));
    assertEquals(Timestamp.valueOf(s1), DefaultHiveSqlDateTimeFormatter.parseTimestamp(s5));
    assertEquals(Timestamp.valueOf(s1), DefaultHiveSqlDateTimeFormatter.parseTimestamp(s6));
  }

  public void testParseTimestampLocalTZ() throws ParseException {
    String s1 ="1970-01-01 03:00:00 America/New_York"; // 8:00 UTC, 00:00 US/Pacific
    String s2 ="1970-01-01 00:00:00.777 US/Pacific";
    String s3 ="1970-01-01 00:00:00.777";
    assertEquals(new TimestampTZ(8 * SECONDS_PER_HOUR, 0, ZoneId.of("US/Pacific")),
        DefaultHiveSqlDateTimeFormatter.parseTimestampLocalTZ(s1, TimeZone.getDefault().toZoneId()));
    assertEquals(new TimestampTZ(8 * SECONDS_PER_HOUR, 777000000, ZoneId.of("US/Pacific")),
        DefaultHiveSqlDateTimeFormatter.parseTimestampLocalTZ(s2));
    assertEquals(new TimestampTZ(8 * SECONDS_PER_HOUR, 777000000, ZoneId.of("US/Pacific")),
        DefaultHiveSqlDateTimeFormatter.parseTimestampLocalTZ(s3, TimeZone.getDefault().toZoneId()));
    
  }

  public void testGetNumberOfTokenGroups() {
    assertEquals(3, DefaultHiveSqlDateTimeFormatter.getNumberOfTokenGroups("2018..39..x"));
    assertEquals(8, DefaultHiveSqlDateTimeFormatter.getNumberOfTokenGroups("2019-01-01T02:03:04Z"));
  }
}
