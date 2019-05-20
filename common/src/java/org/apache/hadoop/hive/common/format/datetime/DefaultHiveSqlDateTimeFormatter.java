package org.apache.hadoop.hive.common.format.datetime;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

/**
 * frogmethod
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
      return formatterDate.format(Timestamp.ofEpochSecond(date.toEpochSecond()));
    } catch (FormatException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String format(TimestampTZ timestampTZ) {
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

  public static Timestamp parseTimestamp(String string) throws ParseException {
    // count number of non-separator tokens
    int numberOfTokenGroups = getNumberOfTokenGroups(string);
    if (!TOKEN_COUNT_FORMATTER_MAP.containsKey(numberOfTokenGroups)) {
      throw new IllegalArgumentException("No available default timestamp parser for input: " + string);
    }
    HiveSqlDateTimeFormatter formatter = TOKEN_COUNT_FORMATTER_MAP.get(numberOfTokenGroups);
    return formatter.parse(string);
  }

  public static TimestampTZ parseTimestampLocalTZ(String string) throws ParseException {
    return parseTimestampLocalTZ(string, null);
  }

  public static TimestampTZ parseTimestampLocalTZ(String string, ZoneId withTimeZone)
      throws ParseException {
    // 1. get time zone from end of string
    boolean zoneIdFound = false;
    ZoneId zoneId;
    String[] stringArray = string.split(" "); //todo rename
    if (stringArray.length < 1 && withTimeZone == null) {
      throw new ParseException("Time zone not provided and not found in string " + string);
    }
    String zoneString = stringArray[stringArray.length - 1];
    try {
       zoneId = ZoneId.of(zoneString);
       zoneIdFound = true;
    } catch (Exception e) {
      if (withTimeZone != null) {
        zoneId = withTimeZone;
      } else {
        throw new ParseException("Time zone not provided and not found in string " + string);
      }
    }
    // 2. parse timestamp part
    String tsString =
        zoneIdFound ? string.substring(0, string.length() - zoneString.length() - 1) : string;
    Timestamp ts = parseTimestamp(tsString);

    ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.ofEpochSecond(ts.toEpochSecond(),
        ts.getNanos(), ZoneOffset.UTC), zoneId);
    if (withTimeZone == null) {
      return new TimestampTZ(zonedDateTime);
    }
    return new TimestampTZ(zonedDateTime.withZoneSameInstant(withTimeZone));
  }

  static int getNumberOfTokenGroups(String string) {
    int index = 0, count = 0;
    boolean lastCharWasSep = true, isIsoToken;

    for (String s : string.split("")) {
      isIsoToken = HiveSqlDateTimeFormatter.VALID_ISO_8601_DELIMITERS.contains(s.toLowerCase());
      if (!HiveSqlDateTimeFormatter.VALID_SEPARATORS.contains(s)) {
        if (lastCharWasSep || isIsoToken) {
          count++;
        }
        // ISO tokens are also delimiters
        lastCharWasSep = isIsoToken;
      } else {
        lastCharWasSep = true;
      }
      index++;
    }
    return count;
  }
}
