package org.apache.hadoop.hive.common.format.datetime;

import org.apache.hadoop.hive.common.type.Timestamp;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * Wrapper for DateTimeFormatter in the java.time package.
 */
public class HiveJavaDateTimeFormat implements HiveDateTimeFormat {

  private DateTimeFormatter formatter;

  @Override public void setFormatter(DateTimeFormatter dateTimeFormatter) {
    this.formatter = dateTimeFormatter;
  }

  @Override public String format(Timestamp ts) {
    return formatter.format(
        LocalDateTime.ofInstant
            (Instant.ofEpochSecond(ts.toEpochSecond(), ts.getNanos()), ZoneId.of("UTC")));
  }

  @Override public Timestamp parse(String string) {
    LocalDateTime ldt = LocalDateTime.parse(string, formatter);
    Instant instant = ldt.toInstant(ZoneId.of("UTC").getRules().getOffset(ldt));
    return Timestamp.ofEpochMilli(instant.toEpochMilli(), instant.getNano());
  }

  // unused methods
  @Override public void setPattern(String pattern) {}
  @Override public void setTimeZone(TimeZone timeZone) {}
  @Override public void setFormatter(SimpleDateFormat simpleDateFormat)
      throws WrongFormatterException {
    throw new WrongFormatterException("HiveJavaDateTimeFormat formatter wraps an object of type" 
        + "java.time.format.DateTimeFormatter, formatter cannot be of type " 
        + "java.text.SimpleDateFormat");
  }
}
