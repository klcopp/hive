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

    verifyPatternParsing("yyyymmdd", new ArrayList<>(List.of(
        ChronoField.YEAR,
        ChronoField.MONTH_OF_YEAR,
        ChronoField.DAY_OF_MONTH
    )));

    verifyPatternParsing("hh24::mi:ss", "hh24::mi:ss".length()-2, new ArrayList<>(List.of(
        ChronoField.HOUR_OF_DAY,
        null,
        ChronoField.MINUTE_OF_HOUR,
        null,
        ChronoField.SECOND_OF_MINUTE
    )));

    verifyPatternParsing("y", 1, new ArrayList<>(List.of(
        ChronoField.YEAR
    )));

    verifyPatternParsing("Y A.M. pm", 11, "y A.M. pm", new ArrayList<>(List.of(
        ChronoField.YEAR,
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
    formatter.setPattern("rr", false);
    Timestamp ts = toTimestamp("2018-02-03 00:00:00");
    assertEquals("18", formatter.format(ts));

    formatter.setPattern("yyyy-mm-ddtsssss.ff4z", false);
    ts = toTimestamp("2018-02-03 00:00:10.777777777");
    assertEquals("2018-02-03T00010.7777Z", formatter.format(ts));

    formatter.setPattern("hh24:mi:ss.ff1", false);
    ts = toTimestamp("2018-02-03 01:02:03.999999999");
    assertEquals("01:02:03.9", formatter.format(ts));

    formatter.setPattern("y hh:mi:ss am a.m. pm p.m. AM A.M. PM P.M.", false);
    ts = toTimestamp("2018-02-03 01:02:03");
    assertEquals("8 01:02:03 am a.m. am a.m. AM A.M. AM A.M.", formatter.format(ts));

    formatter.setPattern("yyyy-mm-ddtsssss.ffz", false);
    ts = toTimestamp("2018-02-03 00:00:10.0070070");
    assertEquals("2018-02-03T00010.007007Z", formatter.format(ts));
  }
  
  public void testFormatDate() {
    //todo frogmethod
  }


  public void testparseTimestamp() {
    formatter.setPattern("yyyy-mm-ddThh24:mi:ss.ff8z", true);
    assertEquals(toTimestamp("2018-02-03 04:05:06.5665"), formatter.parseTimestamp("2018-02-03T04:05:06.5665Z"));

    formatter.setPattern("yyyy-mm-dd hh24:mi:ss.ff", true);
    assertEquals(toTimestamp("2018-02-03 04:05:06.555555555"), formatter.parseTimestamp("2018-02-03 04:05:06.555555555"));

    formatter.setPattern("yy-mm-dd hh12:mi:ss", true);
    assertEquals(toTimestamp("2099-02-03 04:05:06"), formatter.parseTimestamp("99-02-03 04:05:06"));

    formatter.setPattern("rr-mm-dd", true); // test will fail in 2050
    assertEquals(toTimestamp("2000-02-03 00:00:00"), formatter.parseTimestamp("00-02-03"));
    assertEquals(toTimestamp("2049-02-03 00:00:00"), formatter.parseTimestamp("49-02-03"));
    assertEquals(toTimestamp("1950-02-03 00:00:00"), formatter.parseTimestamp("50-02-03"));

    formatter.setPattern("rrrr-mm-dd", true); // test will fail in 2050
    assertEquals(toTimestamp("2000-02-03 00:00:00"), formatter.parseTimestamp("00-02-03"));
    assertEquals(toTimestamp("2049-02-03 00:00:00"), formatter.parseTimestamp("49-02-03"));
    assertEquals(toTimestamp("1950-02-03 00:00:00"), formatter.parseTimestamp("50-02-03"));

    formatter.setPattern("yyy-mm-dd", true);
    assertEquals(toTimestamp("2018-01-01 00:00:00"), formatter.parseTimestamp("018-01-01"));

    formatter.setPattern("yyyyddd", true);
    assertEquals(toTimestamp("2018-10-11 00:00:00"), formatter.parseTimestamp("2018284"));

    formatter.setPattern("yyyyddd", true);
    assertEquals(toTimestamp("2018-01-04 00:00:00"), formatter.parseTimestamp("20184"));

    formatter.setPattern("yyyy-mm-ddThh24:mi:ss.ffz", true);
    assertEquals(toTimestamp("2018-02-03 04:05:06.444"), formatter.parseTimestamp("2018------02-03t04:05:06.444Z"));

    formatter.setPattern("hh:mi:ss A.M.", true);
    assertEquals(toTimestamp("1970-01-01 16:05:06"), formatter.parseTimestamp("04:05:06 P.M."));

    formatter.setPattern("YYYY-MM-DD HH24:MI TZH:TZM", true);
    assertEquals(toTimestamp("2019-01-01 15:30:00"), formatter.parseTimestamp("2019-1-1 14:00 -1:30"));

    formatter.setPattern("YYYY-MM-DD HH24:MI TZH:TZM", true);
    assertEquals(toTimestamp("2019-01-01 12:30:00"), formatter.parseTimestamp("2019-1-1 14:00-1:30"));

    formatter.setPattern("TZM:TZH", true);
    assertEquals(toTimestamp("1970-01-01 03:01:00"), formatter.parseTimestamp("1 -3"));

    formatter.setPattern("TZHYYY-MM-DD", true);
    assertEquals(toTimestamp("2333-01-01 13:00:00"), formatter.parseTimestamp("11333-01-02"));

    formatter.setPattern("YYYY-MM-DD HH12:MI AM", true);
    assertEquals(toTimestamp("2019-01-01 23:00:00"), formatter.parseTimestamp("2019-01-01 11:00 p.m."));

    formatter.setPattern("YYYY-MM-DD HH12:MI A.M.", true);
    assertEquals(toTimestamp("2019-01-01 23:00:00"), formatter.parseTimestamp("2019-01-01 11:00 pm"));
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
