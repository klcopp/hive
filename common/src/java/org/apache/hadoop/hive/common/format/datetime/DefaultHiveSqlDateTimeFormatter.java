package org.apache.hadoop.hive.common.format.datetime;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;

import java.time.LocalDateTime;
import java.time.ZoneId;
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
    return (ts.getNanos() == 0) ? formatterNoNanos.format(ts) : formatterWithNanos.format(ts);
  }

  public static String format(Date date) {
    return formatterDate.format(Timestamp.ofEpochSecond(date.toEpochSecond()));
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

//    HiveSqlDateTimeFormatter.VALID_SEPARATORS

    //todo count number of non-separator tokens
    int count = 0; //todo rename
    
    // go through string, and count last string was separator

    if (!TOKEN_COUNT_FORMATTER_MAP.containsKey(count)) {
      throw new IllegalArgumentException("");//frogmethod todo
    }
    HiveSqlDateTimeFormatter formatter = TOKEN_COUNT_FORMATTER_MAP.get(count);
    return formatter.parse(string);
  }

  //todo
  public static TimestampTZ parseTimestampLocalTZ(String string) throws ParseException {
    return null;
  }
}
