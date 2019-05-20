package org.apache.hadoop.hive.common.format.datetime;

import junit.framework.TestCase;
import org.apache.hadoop.hive.common.type.Timestamp;

public class TestDefaultHiveSqlDateTimeFormatter extends TestCase {


  public void testFormatTimestamp() {
    String s1 = "2019-01-01 02:03:04.44444";
    String s2 = "2019-01-01 02:03:04";
    assertEquals(s1, DefaultHiveSqlDateTimeFormatter.format(Timestamp.valueOf(s1)));
    assertEquals(s2, DefaultHiveSqlDateTimeFormatter.format(Timestamp.valueOf(s2)));
  }

  public void testFormatDate() {
    
  }

  public void testFormatTimestampTZ() {
    
  }

  public void testParseTimestamp() {
    
  }

  public void testParseTimestampLocalTZ() {
    
  }
}
