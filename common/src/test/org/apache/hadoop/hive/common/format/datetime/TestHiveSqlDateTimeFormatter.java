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
import org.junit.Test;

import java.util.ArrayList;

/**
 * Test class for HiveSqlDateTimeFormatter.
 */

public class TestHiveSqlDateTimeFormatter extends TestCase {

  private HiveSqlDateTimeFormatter formatter = new HiveSqlDateTimeFormatter();

  @Test
  public void testSetPattern() throws IllegalArgumentException{
    verifyPatternParsing(" ---yyyy-\'-:-  -,.;/MM-dd--", 27, new ArrayList<>(List.of(
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR,
        HiveSqlDateTimeFormatter.TokenType.YEAR,
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR,
        HiveSqlDateTimeFormatter.TokenType.MONTH,
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR,
        HiveSqlDateTimeFormatter.TokenType.DAY_OF_MONTH,
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR
        )));

    verifyPatternParsing("yyyymmdd", 8, new ArrayList<>(List.of(
        HiveSqlDateTimeFormatter.TokenType.YEAR,
        HiveSqlDateTimeFormatter.TokenType.MONTH,
        HiveSqlDateTimeFormatter.TokenType.DAY_OF_MONTH
    )));

    verifyPatternParsing("hh:mi:ss", 8, new ArrayList<>(List.of(
        HiveSqlDateTimeFormatter.TokenType.HOUR_IN_HALF_DAY,
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR,
        HiveSqlDateTimeFormatter.TokenType.MINUTE,
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR,
        HiveSqlDateTimeFormatter.TokenType.SECOND
    )));
    verifyPatternParsing("y", 1, new ArrayList<>(List.of(
        HiveSqlDateTimeFormatter.TokenType.YEAR
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

  private void verifyPatternParsing(String pattern, int expectedLength, ArrayList<HiveSqlDateTimeFormatter.TokenType> expected)
      throws IllegalArgumentException {
    formatter.setPattern(pattern, true);
    assertEquals(expected.size(), formatter.tokens.size());
    StringBuilder sb = new StringBuilder();
    int actualLength = 0;
    for (int i = 0; i < expected.size(); i++) {
      assertEquals("Generated list of tokens not correct", expected.get(i), formatter.tokens.get(i).type);
      sb.append(formatter.tokens.get(i).string);
      actualLength += formatter.tokens.get(i).length;
    }
    assertEquals("Token strings concatenated don't match original pattern string", pattern.toLowerCase(), sb.toString());
    assertEquals(expectedLength, actualLength);
  }

  @Test
  public void testFormat() {
  }

  @Test
  public void testParse() throws ParseException {
  }
}
