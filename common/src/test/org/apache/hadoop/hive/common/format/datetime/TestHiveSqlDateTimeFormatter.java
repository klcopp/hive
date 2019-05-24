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
    verifyBadPattern("yyyyTy", true); // too many years
    verifyBadPattern("yyyyTr", true); // both year and round year provided
    verifyBadPattern("tzm", false);
    verifyBadPattern("tzh", false);
  }

  public void testFormatTimestamp() {
    checkFormatTs("rr", "2018-02-03 00:00:00", "18");
    checkFormatTs("yyyy-mm-ddtsssss.ff4z", "2018-02-03 00:00:10.777777777", "2018-02-03T00010.7777Z");
    checkFormatTs("hh24:mi:ss.ff1", "2018-02-03 01:02:03.999999999", "01:02:03.9");
    checkFormatTs("y hh:mi:ss am a.m. pm p.m. AM A.M. PM P.M.", "2018-02-03 01:02:03", "8 01:02:03 am a.m. am a.m. AM A.M. AM A.M.");
    checkFormatTs("yyyy-mm-ddtsssss.ffz", "2018-02-03 00:00:10.0070070", "2018-02-03T00010.007007Z");
  }

  private void checkFormatTs(String pattern, String input, String expectedOutput) {
    formatter.setPattern(pattern, false);
    assertEquals(expectedOutput, formatter.format(toTimestamp(expectedOutput)));
  }

  public void testFormatDate() {
    //todo frogmethod
  }

  public void testparseTimestamp() {
    checkParseTimestamp("yyyy-mm-ddThh24:mi:ss.ff8z", "2018-02-03T04:05:06.5665Z", "2018-02-03 04:05:06.5665");
    checkParseTimestamp("yyyy-mm-dd hh24:mi:ss.ff", "2018-02-03 04:05:06.555555555", "2018-02-03 04:05:06.555555555");
    checkParseTimestamp("yy-mm-dd hh12:mi:ss", "99-02-03 04:05:06", "2099-02-03 04:05:06");
    checkParseTimestamp("rr-mm-dd", "00-02-03", "2000-02-03 00:00:00");
    checkParseTimestamp("rr-mm-dd", "49-02-03", "2049-02-03 00:00:00");
    checkParseTimestamp("rr-mm-dd", "50-02-03", "1950-02-03 00:00:00");
    checkParseTimestamp("rrrr-mm-dd", "00-02-03", "2000-02-03 00:00:00");
    checkParseTimestamp("rrrr-mm-dd", "49-02-03", "2049-02-03 00:00:00");
    checkParseTimestamp("rrrr-mm-dd", "50-02-03", "1950-02-03 00:00:00");
    checkParseTimestamp("yyy-mm-dd","018-01-01","2018-01-01 00:00:00");
    checkParseTimestamp("yyyyddd", "2018284","2018-10-11 00:00:00");
    checkParseTimestamp("yyyyddd", "20184","2018-01-04 00:00:00");
    checkParseTimestamp("yyyy-mm-ddThh24:mi:ss.ffz", "2018------02-03t04:05:06.444Z","2018-02-03 04:05:06.444");
    checkParseTimestamp("hh:mi:ss A.M.", "04:05:06 P.M.","1970-01-01 16:05:06");
    checkParseTimestamp("YYYY-MM-DD HH24:MI TZH:TZM", "2019-1-1 14:00 -1:30","2019-01-01 15:30:00");
    checkParseTimestamp("YYYY-MM-DD HH24:MI TZH:TZM", "2019-1-1 14:00-1:30","2019-01-01 12:30:00");
    checkParseTimestamp("TZM:TZH", "1 -3","1970-01-01 03:01:00");
    checkParseTimestamp("TZHYYY-MM-DD", "11333-01-02","2333-01-01 13:00:00");
    checkParseTimestamp("YYYY-MM-DD HH12:MI AM", "2019-01-01 11:00 p.m.","2019-01-01 23:00:00");
    checkParseTimestamp("YYYY-MM-DD HH12:MI A.M.", "2019-01-01 11:00 pm","2019-01-01 23:00:00");
  }
  
  private void checkParseTimestamp(String pattern, String input, String expectedOutput) {
    formatter.setPattern(pattern, true);
    assertEquals(toTimestamp(expectedOutput), formatter.parseTimestamp(input));
  }

  public void testparseDate() {
    //todo frogmethod
  }

  public void testParseTimestampError() {
    verifyBadParseString("yyyy", "2019-02-03");
    verifyBadParseString("yyyy-mm-dd  ", "2019-02-03");
    verifyBadParseString("yyyy-mm-dd", "2019-02-03...");
    verifyBadParseString("yyyymmddhh12miss", "2018020304:05:06 America/New_York");
  }

  private void verifyBadPattern(String string, boolean forParsing) {
    try {
      formatter.setPattern(string, forParsing);
      fail();
    } catch (Exception e) {
      assertEquals(e.getClass().getName(), IllegalArgumentException.class.getName());
    }
  }

  private void verifyPatternParsing(String pattern, ArrayList<TemporalField> expected) {
    verifyPatternParsing(pattern, pattern.length(), expected);
  }

  private void verifyPatternParsing(String pattern, int expectedPatternLength,
      ArrayList<TemporalField> expected) {
    verifyPatternParsing(pattern, expectedPatternLength, pattern.toLowerCase(), expected);
  }

  private void verifyPatternParsing(String pattern, int expectedPatternLength,
      String expectedPattern, ArrayList<TemporalField> expected)  {
    formatter.setPattern(pattern, false);
    assertEquals(expected.size(), formatter.tokens.size());
    StringBuilder sb = new StringBuilder();
    int actualPatternLength = 0;
    for (int i = 0; i < expected.size(); i++) {
      assertEquals("Generated list of tokens not correct", expected.get(i),
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
