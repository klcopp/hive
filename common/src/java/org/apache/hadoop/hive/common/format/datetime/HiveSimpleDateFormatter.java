package org.apache.hadoop.hive.common.format.datetime;

import org.apache.hadoop.hive.common.type.Timestamp;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

/**
 * Wrapper for java.text.SimpleDateFormat.
 */
public class HiveSimpleDateFormatter implements HiveDateTimeFormatter {
  
  private SimpleDateFormat format = new SimpleDateFormat();

  public HiveSimpleDateFormatter() {}
  
  @Override public void setFormatter(SimpleDateFormat simpleDateFormat) {
    this.format = simpleDateFormat;
  }

  @Override public String format(Timestamp ts) {
    Date date = new Date(ts.toEpochMilli());
    return format.format(date);
  }

  @Override public Timestamp parse(String string) throws ParseException {
    try {
      Date date = format.parse(string);
      return Timestamp.ofEpochMilli(date.getTime());
    } catch (java.text.ParseException e) {
      throw new ParseException(
          "String " + string + " could not be parsed by java.text.SimpleDateFormat: " + format);
    }
  }

  @Override public void setPattern(String pattern) {
    format.applyPattern(pattern);
  }

  @Override public void setTimeZone(TimeZone timeZone) {
    format.setTimeZone(timeZone);
  }

  /// unused methods
  @Override public void setFormatter(DateTimeFormatter dateTimeFormatter)
      throws WrongFormatterException {
    throw new WrongFormatterException(
        "HiveSimpleDateFormatter formatter wraps an object of type java.text.SimpleDateFormat, " 
            + "formatter cannot be of type java.time.format.DateTimeFormatter");
  }

}
