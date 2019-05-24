/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.common.format.datetime;

import com.sun.tools.javac.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;
import java.util.ArrayList;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * Test class for HiveSqlDateTimeFormatter.
 */

public class TestHiveSqlDateTimeFormatter extends TestCase {

  private HiveSqlDateTimeFormatter formatter = new HiveSqlDateTimeFormatter();

  public void testSetPattern() {
    verifyPatternParsing(" ---yyyy-\'-:-  -,.;/MM-dd--", new ArrayList<>(List.of(
        null,
        ChronoField.YEAR,
        null,
        ChronoField.MONTH_OF_YEAR,
        null,
        ChronoField.DAY_OF_MONTH,
        null
        )));

    verifyPatternParsing("ymmdddhh24::mi:ss A.M. pm", 25, "ymmdddhh24::mi:ss A.M. pm",
        new ArrayList<>(List.of(
        ChronoField.YEAR,
        ChronoField.MONTH_OF_YEAR,
        ChronoField.DAY_OF_YEAR,
        ChronoField.HOUR_OF_DAY,
        null, ChronoField.MINUTE_OF_HOUR,
        null, ChronoField.SECOND_OF_MINUTE,
        null, ChronoField.AMPM_OF_DAY,
        null, ChronoField.AMPM_OF_DAY
    )));
  }

  public void testSetPatternWithBadPatterns() {
    verifyBadPattern("e", true);
    verifyBadPattern("yyyy-1", true);

    verifyBadPattern("yyyy Y", true);
    verifyBadPattern("yyyy R", true);
    verifyBadPattern("yyyy-MM-DDD", true);
    verifyBadPattern("yyyy-mm-DD DDD", true);
    verifyBadPattern("yyyy-mm-dd HH24 HH12", true);
    verifyBadPattern("yyyy-mm-dd HH24 AM", true);
    verifyBadPattern("yyyy-mm-dd HH24 SSSSS", true);
    verifyBadPattern("yyyy-mm-dd HH12 SSSSS", true);
    verifyBadPattern("yyyy-mm-dd SSSSS AM", true);
    verifyBadPattern("yyyy-mm-dd MI SSSSS", true);
    verifyBadPattern("yyyy-mm-dd SS SSSSS", true);

    verifyBadPattern("tzm", false);
    verifyBadPattern("tzh", false);
  }

  public void testFormatTimestamp() {
    checkFormatTs("rr rrrr ddd", "2018-01-03 00:00:00", "18 2018 003");
    checkFormatTs("yyyy-mm-ddtsssss.ff4z", "2018-02-03 00:00:10.777777777", "2018-02-03T00010.7777Z");
    checkFormatTs("hh24:mi:ss.ff1", "2018-02-03 01:02:03.999999999", "01:02:03.9");
    checkFormatTs("y yyy hh:mi:ss.ffz", "2018-02-03 01:02:03.0070070", "8 018 01:02:03.007007Z");
    checkFormatTs("am a.m. pm p.m. AM A.M. PM P.M.", "2018-02-03 01:02:03.0070070", "am a.m. am a.m. AM A.M. AM A.M.");
  }

  private void checkFormatTs(String pattern, String input, String expectedOutput) {
    formatter.setPattern(pattern, false);
    assertEquals(expectedOutput, formatter.format(toTimestamp(input)));
  }

  public void testFormatDate() {
    checkFormatDate("rr rrrr ddd", "2018-01-03", "18 2018 003");
    checkFormatDate("yyyy-mm-ddtsssss.ff4z", "2018-02-03", "2018-02-03T00000.0000Z");
    checkFormatDate("hh24:mi:ss.ff1", "2018-02-03", "00:00:00.0");
    checkFormatDate("y yyy T hh:mi:ss.ffz", "2018-02-03", "8 018 T 00:00:00.0Z");
    checkFormatDate("am a.m. pm p.m. AM A.M. PM P.M.", "2018-02-03", "am a.m. am a.m. AM A.M. AM A.M.");
    checkFormatDate("DDD", "2019-12-31", "365");
    checkFormatDate("DDD", "2020-12-31", "366");
  }
  
  private void checkFormatDate(String pattern, String input, String expectedOutput) {
    formatter.setPattern(pattern, false);
    assertEquals(expectedOutput, formatter.format(toDate(input)));
  }

  public void testParseTimestamp() {
    checkParseTimestamp("yyyy-mm-ddThh24:mi:ss.ff8z", "2018-02-03T04:05:06.5665Z", "2018-02-03 04:05:06.5665");
    checkParseTimestamp("yyyy-mm-dd hh24:mi:ss.ff", "2018-02-03 04:05:06.555555555", "2018-02-03 04:05:06.555555555");
    checkParseTimestamp("yy-mm-dd hh12:mi:ss", "99-2-03 04:05:06", "2099-02-03 04:05:06");
    checkParseTimestamp("rr-mm-dd", "00-02-03", "2000-02-03 00:00:00");
    checkParseTimestamp("rr-mm-dd", "49-02-03", "2049-02-03 00:00:00");
    checkParseTimestamp("rr-mm-dd", "50-02-03", "1950-02-03 00:00:00");
    checkParseTimestamp("rrrr-mm-dd", "00-02-03", "2000-02-03 00:00:00");
    checkParseTimestamp("rrrr-mm-dd", "49-02-03", "2049-02-03 00:00:00");
    checkParseTimestamp("rrrr-mm-dd", "50-02-03", "1950-02-03 00:00:00");
    checkParseTimestamp("yyy-mm-dd","018-01-01","2018-01-01 00:00:00");
    checkParseTimestamp("yyyyddd", "2018284","2018-10-11 00:00:00");
    checkParseTimestamp("yyyyddd", "20184","2018-01-04 00:00:00");
    checkParseTimestamp("yyyy-mm-ddThh24:mi:ss.ffz", "2018-02-03t04:05:06.444Z","2018-02-03 04:05:06.444");
    checkParseTimestamp("hh:mi:ss A.M.", "04:05:06 P.M.","1970-01-01 16:05:06");
    checkParseTimestamp("YYYY-MM-DD HH24:MI TZH:TZM", "2019-1-1 14:00--1:-30","2019-01-01 15:30:00");
    checkParseTimestamp("YYYY-MM-DD HH24:MI TZH:TZM", "2019-1-1 14:00-1:30","2019-01-01 12:30:00");
    checkParseTimestamp("TZM:TZH", "1 -3","1970-01-01 03:01:00");
    checkParseTimestamp("TZH:TZM", "-0:30","1970-01-01 00:30:00");
    checkParseTimestamp("TZM/YYY-MM-TZH/DD", "0/333-01-11/02","2333-01-01 13:00:00");
    checkParseTimestamp("YYYY-MM-DD HH12:MI AM", "2019-01-01 11:00 p.m.","2019-01-01 23:00:00");
    checkParseTimestamp("YYYY-MM-DD HH12:MI A.M..", "2019-01-01 11:00 pm.","2019-01-01 23:00:00");

    //Test "day in year" token in a leap year scenario
    checkParseTimestamp("YYYY DDD", "2000 60", "2000-02-29 00:00:00");
    checkParseTimestamp("YYYY DDD", "2000 61", "2000-03-01 00:00:00");
    checkParseTimestamp("YYYY DDD", "2000 366", "2000-12-31 00:00:00");
    //Test timezone offset parsing without separators
    checkParseTimestamp("YYYYMMDDHH12MIA.M.TZHTZM", "201812310800AM+0515", "2018-12-31 02:45:00");
    checkParseTimestamp("YYYYMMDDHH12MIA.M.TZHTZM", "201812310800AM0515", "2018-12-31 02:45:00");
    checkParseTimestamp("YYYYMMDDHH12MIA.M.TZHTZM", "201812310800AM-0515", "2018-12-31 13:15:00");
  }
  
  private void checkParseTimestamp(String pattern, String input, String expectedOutput) {
    formatter.setPattern(pattern, true);
    assertEquals(toTimestamp(expectedOutput), formatter.parseTimestamp(input));
  }

  public void testParseDate() {
    checkParseDate("yyyy-mm-dd hh mi ss", "2018/01/01 2.2.2", "2018-01-01");
    checkParseDate("rr-mm-dd", "00-02-03", "2000-02-03");
    checkParseDate("rr-mm-dd", "49-02-03", "2049-02-03");
    checkParseDate("rr-mm-dd", "50-02-03", "1950-02-03");
  }

  private void checkParseDate(String pattern, String input, String expectedOutput) {
    formatter.setPattern(pattern, true);
    assertEquals(toDate(expectedOutput), formatter.parseDate(input));
  }

  public void testParseTimestampError() {
    verifyBadParseString("yyyy", "2019-02-03");
    verifyBadParseString("yyyy-mm-dd  ", "2019-02-03"); //separator missing
    verifyBadParseString("yyyy-mm-dd", "2019-02-03..."); //extra separators
    verifyBadParseString("yyyy-mm-dd hh12:mi:ss", "2019-02-03 14:00:00"); //hh12 out of range
    verifyBadParseString("yyyy-dddsssss", "2019-912345");
    verifyBadParseString("yyyy-mm-dd", "2019-13-23"); //mm out of range
    verifyBadParseString("yyyy-mm-dd tzh:tzm", "2019-01-01 +16:00"); //tzh out of range
    verifyBadParseString("yyyy-mm-dd tzh:tzm", "2019-01-01 +14:60"); //tzm out of range
    verifyBadParseString("YYYY DDD", "2000 367"); //ddd out of range
  }

  private void verifyBadPattern(String string, boolean forParsing) {
    try {
      formatter.setPattern(string, forParsing);
      fail();
    } catch (Exception e) {
      assertEquals(e.getClass().getName(), IllegalArgumentException.class.getName());
    }
  }

  /**
   * Checks:
   * -token.temporalField for each token
   * -sum of token.lengths
   * -concatenation of token.strings
   */
  private void verifyPatternParsing(String pattern, ArrayList<TemporalField> temporalFields) {
    verifyPatternParsing(pattern, pattern.length(), pattern.toLowerCase(), temporalFields);
  }

  private void verifyPatternParsing(String pattern, int expectedPatternLength,
      String expectedPattern, ArrayList<TemporalField> temporalFields)  {
    formatter.setPattern(pattern, false);
    assertEquals(temporalFields.size(), formatter.tokens.size());
    StringBuilder sb = new StringBuilder();
    int actualPatternLength = 0;
    for (int i = 0; i < temporalFields.size(); i++) {
      assertEquals("Generated list of tokens not correct", temporalFields.get(i),
          formatter.tokens.get(i).temporalField);
      sb.append(formatter.tokens.get(i).string);
      actualPatternLength += formatter.tokens.get(i).length;
    }
    assertEquals("Token strings concatenated don't match original pattern string",
        expectedPattern, sb.toString());
    assertEquals(expectedPatternLength, actualPatternLength);
  }

  private void verifyBadParseString(String pattern, String string) {
    try {
      formatter.setPattern(pattern, true);
      formatter.parseTimestamp(string);
      fail();
    } catch (Exception e) {
      assertEquals(e.getClass().getName(), IllegalArgumentException.class.getName());
    }
  }


  // Methods that construct datetime objects using java.time.DateTimeFormatter.

  public static Date toDate(String s) {
    LocalDate localDate = LocalDate.parse(s, DATE_FORMATTER);
    return Date.ofEpochDay((int) localDate.toEpochDay());
  }
  
  /**
   * This is effectively the old Timestamp.valueOf method.
   */
  public static Timestamp toTimestamp(String s) {
    LocalDateTime localDateTime = LocalDateTime.parse(s.trim(), TIMESTAMP_FORMATTER);
    return Timestamp.ofEpochSecond(
        localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano());
  }

  private static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private static final DateTimeFormatter TIMESTAMP_FORMATTER;
  static {
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
    builder.appendValue(YEAR, 1, 10, SignStyle.NORMAL).appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL).appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
        .optionalStart().appendLiteral(" ")
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NORMAL).appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL).appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
        .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
        .optionalEnd();
    TIMESTAMP_FORMATTER = builder.toFormatter().withResolverStyle(ResolverStyle.LENIENT);
  }
}
