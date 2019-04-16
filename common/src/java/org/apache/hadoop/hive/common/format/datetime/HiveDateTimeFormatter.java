package org.apache.hadoop.hive.common.format.datetime;

import org.apache.hadoop.hive.common.type.Timestamp;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

public interface HiveDateTimeFormatter {

  /**
   * Only used for HiveSimpleDateFormatter, which is a wrapper for the given SimpleDateFormat object.
   */
  void setFormatter(SimpleDateFormat simpleDateFormat) throws WrongFormatterException;

  /**
   * Only used for HiveJavaDateTimeFormatter, which is a wrapper for the given DateTimeFormatter object.
   */
  void setFormatter(DateTimeFormatter dateTimeFormatter) throws WrongFormatterException;

  /**
   * Format the given timestamp into a string.
   */
  String format(Timestamp ts);

  /**
   * Parse the given string into a timestamp.
   * 
   * @throws ParseException if string cannot be parsed.
   */
  Timestamp parse(String string) throws ParseException;

  /**
   * Set the format pattern to be used for formatting timestamps or parsing strings.
   * Different HiveDateTimeFormatter implementations interpret some patterns differently. For example,
   * HiveSimpleDateFormatter interprets the string "mm" as minute, while HiveSqlDateTimeFormatter
   * interprets it as month.
   */
  void setPattern(String pattern);

  /**
   * Set the time zone of the formatter. Only HiveSimpleDateFormatter uses this.
   */
  void setTimeZone(TimeZone timeZone);

  public enum FormatterType {
    SIMPLE_DATE_FORMAT,
    JAVA_TIME_DATETIMEFORMATTER,
    SQL_2016
  }
  
  
}
