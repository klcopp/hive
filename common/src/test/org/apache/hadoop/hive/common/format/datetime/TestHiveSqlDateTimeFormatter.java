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
import org.apache.hadoop.hive.common.type.Timestamp;
import org.junit.Test;

import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;
import java.util.ArrayList;

/**
 * Test class for HiveSqlDateTimeFormatter.
 */

public class TestHiveSqlDateTimeFormatter extends TestCase {

  private HiveSqlDateTimeFormatter formatter = new HiveSqlDateTimeFormatter();

  @Test
  public void testSetPattern() throws IllegalArgumentException{
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

    verifyPatternParsing("hh24::mi:ss", new ArrayList<>(List.of(
        ChronoField.HOUR_OF_DAY,
        null,
        ChronoField.MINUTE_OF_HOUR,
        null,
        ChronoField.SECOND_OF_MINUTE
    )));
    verifyPatternParsing("y", new ArrayList<>(List.of(
        ChronoField.YEAR
    )));
  }

  public void testSetPatternWithBadPatterns() {
    verifyBadPattern("e");
    verifyBadPattern("yyyy-1");
    verifyBadPattern("yyyyy"); // too many years
  }

  private void verifyBadPattern(String string) {
    try {
      formatter.setPattern(string, true);
      fail();
    } catch (Exception e) {
      assertEquals(e.getClass().getName(), ParseException.class.getName());
    }
  }

  private void verifyPatternParsing(String pattern, ArrayList<TemporalField> expected)
      throws IllegalArgumentException {
    formatter.setPattern(pattern, true);
    assertEquals(expected.size(), formatter.tokens.size());
    StringBuilder sb = new StringBuilder();
    int actualPatternLength = 0;
    for (int i = 0; i < expected.size(); i++) {
      assertEquals("Generated list of tokens not correct", expected.get(i), formatter.tokens.get(i).temporalField);
      sb.append(formatter.tokens.get(i).string);
      actualPatternLength += formatter.tokens.get(i).length;
    }
    assertEquals("Token strings concatenated don't match original pattern string", pattern.toLowerCase(), sb.toString());
    assertEquals(pattern.length(), actualPatternLength);
  }

  @Test
  public void testFormat() {
    formatter.setPattern("yyyy", false);
    Timestamp ts = Timestamp.valueOf("2018-01-01 00:00:00");
    assertEquals("2018", formatter.format(ts));

    formatter.setPattern("yyyy-mm-dd", false);
    ts = Timestamp.valueOf("2018-02-03 00:00:00");
    assertEquals("2018-02-03", formatter.format(ts));

    formatter.setPattern("hh24:mi:ss", false);
    ts = Timestamp.valueOf("2018-02-03 01:02:03");
    assertEquals("01:02:03", formatter.format(ts));
  }

  @Test
  public void testParse() throws ParseException {
    formatter.setPattern("yy-mm-dd hh24:mi:ss", true);
    assertEquals(Timestamp.valueOf("2018-02-03 04:05:06"), formatter.parse("18-02-03 04:05:06"));

    formatter.setPattern("yyyy-mm-dd", true);
    assertEquals(Timestamp.valueOf("2018-01-01 00:00:00"), formatter.parse("2018-01-01"));

    formatter.setPattern("yyyy", true);
    assertEquals(Timestamp.valueOf("2018-01-01 00:00:00"), formatter.parse("2018"));
  }
}
