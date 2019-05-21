package org.apache.hadoop.hive.common.format.datetime;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

/**
 * Parse/format datetime objects according to some select default formats.
 */
public class DefaultHiveSqlDateTimeFormatter {

  private static HiveSqlDateTimeFormatter formatterDate = new HiveSqlDateTimeFormatter();
  private static HiveSqlDateTimeFormatter formatterNoNanos = new HiveSqlDateTimeFormatter();
  private static HiveSqlDateTimeFormatter formatterWithNanos = new HiveSqlDateTimeFormatter();
  private static HiveSqlDateTimeFormatter formatterIsoNoNanos = new HiveSqlDateTimeFormatter();
  private static HiveSqlDateTimeFormatter formatterIsoWithNanos = new HiveSqlDateTimeFormatter();
  
  static {
    //forParsing is false because there's no need to verify pattern
    formatterDate.setPattern("yyyy-mm-dd", false);
    formatterNoNanos.setPattern("yyyy-mm-dd hh24:mi:ss", false);
    formatterWithNanos.setPattern("yyyy-mm-dd hh24:mi:ss.ff", false);
    formatterIsoNoNanos.setPattern("yyyy-mm-ddThh24:mi:ssZ", false);
    formatterIsoWithNanos.setPattern("yyyy-mm-ddThh24:mi:ss.ffZ", false);
  }

  private static final Map<Integer, HiveSqlDateTimeFormatter> TOKEN_COUNT_FORMATTER_MAP =
      ImmutableMap.<Integer, HiveSqlDateTimeFormatter>builder().put(3, formatterDate)
          .put(6, formatterNoNanos).put(7, formatterWithNanos).put(8, formatterIsoNoNanos)
          .put(9, formatterIsoWithNanos).build();

  public static String format(Timestamp ts) {
    try {
      return (ts.getNanos() == 0) ? formatterNoNanos.format(ts) : formatterWithNanos.format(ts);
    } catch (FormatException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String format(Date date) {
    try {
      return formatterDate.format(date);
    } catch (FormatException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String format(TimestampTZ timestampTZ) { //todo
    String output;

    //add local time part
    ZoneId zoneId = timestampTZ.getZonedDateTime().getZone();
    LocalDateTime ldt = timestampTZ.getZonedDateTime().toLocalDateTime();
    Timestamp ts = Timestamp.ofEpochSecond(
        ldt.toEpochSecond(zoneId.getRules().getOffset(ldt)), ldt.getNano());
    output = format(ts);

    //add the time zone part
    output += " " + zoneId;
    return output;
  }

  public static Timestamp parseTimestamp(String input) throws ParseException {
    // count number of non-separator tokens
    int numberOfTokenGroups = getNumberOfTokenGroups(input);
    if (!TOKEN_COUNT_FORMATTER_MAP.containsKey(numberOfTokenGroups)) {
      throw new IllegalArgumentException("No available default timestamp parser for input: " + input);
    }
    HiveSqlDateTimeFormatter formatter = TOKEN_COUNT_FORMATTER_MAP.get(numberOfTokenGroups);
    return formatter.parseTimestamp(input);
  }
  
  public static Date parseDate(String input) throws ParseException {
    return formatterDate.parseDate(input);
  }

  public static TimestampTZ parseTimestampTZ(String input, ZoneId withTimeZone)
      throws ParseException {
    // count number of non-separator tokens
    int numberOfTokenGroups = getNumberOfTokenGroups(input);
    // try numberOfTokenGroups = numberOfTokenGroups-1 (in case time zone was Turkey or Zulu)
    for (int i = numberOfTokenGroups; i >= numberOfTokenGroups - 1; i--) {
      try {
        if (TOKEN_COUNT_FORMATTER_MAP.containsKey(i)) {
          return TOKEN_COUNT_FORMATTER_MAP.get(i).parseTimestampTZ(input, withTimeZone);
        }
      } catch (ParseException e) {
        // keep looping
      }
    }
    throw new ParseException(input + " couldn't be parsed to timestamp with local time zone.");
  }

  static int getNumberOfTokenGroups(String input) {
    int count = 0;
    boolean lastCharWasSep = true, isIsoDelimiter;

    for (char c : input.toCharArray()) {
      String s = String.valueOf(c);
      isIsoDelimiter = HiveSqlDateTimeFormatter.VALID_ISO_8601_DELIMITERS.contains(s.toLowerCase());
      if (!HiveSqlDateTimeFormatter.VALID_SEPARATORS.contains(s)) {
        if (!isIsoDelimiter && !Character.isDigit(c)) { // it's probably part of a time zone. Halt.
          break;
        }
        if (lastCharWasSep || isIsoDelimiter ) {
          count++;
        }
        // ISO delimiters are... delimiters
        lastCharWasSep = isIsoDelimiter;
      } else {
        lastCharWasSep = true;
      }
    }
    return count;
  }
}
