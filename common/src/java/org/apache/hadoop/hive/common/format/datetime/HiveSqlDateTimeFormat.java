package org.apache.hadoop.hive.common.format.datetime;

import org.apache.hadoop.hive.common.type.Timestamp;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * Formatter using SQL:2016 datetime patterns.
 */

public class HiveSqlDateTimeFormat implements HiveDateTimeFormat {
  
  private String pattern;

  public HiveSqlDateTimeFormat() {}
  
  @Override public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  @Override public String format(Timestamp ts) {
    //TODO
    return null;
  }

  @Override public Timestamp parse(String string) throws ParseException {
    //TODO
    return null;
  }

  // unused methods
  @Override public void setTimeZone(TimeZone timeZone) {}
  @Override public void setFormatter(DateTimeFormatter dateTimeFormatter)
      throws WrongFormatterException {
    throw new WrongFormatterException("HiveSqlDateTimeFormat is not a wrapper for " 
        + "java.time.format.DateTimeFormatter, use HiveJavaDateTimeFormat instead.");
  }
  @Override public void setFormatter(SimpleDateFormat simpleDateFormat)
      throws WrongFormatterException {
    throw new WrongFormatterException("HiveSqlDateTimeFormat is not a wrapper for " 
        + "java.text.SimpleDateFormat, use HiveSimpleDateFormat instead.");
  }
}
