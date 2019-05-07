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
  public void testSetPattern() throws ParseException{
    checkTokenList("---yyyy-----MM-dd--", new ArrayList<>(List.of(
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR,
        HiveSqlDateTimeFormatter.TokenType.YEAR,
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR,
        HiveSqlDateTimeFormatter.TokenType.MONTH,
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR,
        HiveSqlDateTimeFormatter.TokenType.DAY_OF_MONTH,
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR

        )));

    checkTokenList("yyyymmdd", new ArrayList<>(List.of(
        HiveSqlDateTimeFormatter.TokenType.YEAR,
        HiveSqlDateTimeFormatter.TokenType.MONTH,
        HiveSqlDateTimeFormatter.TokenType.DAY_OF_MONTH
    )));

    checkTokenList("hh:mi:ss", new ArrayList<>(List.of(
        HiveSqlDateTimeFormatter.TokenType.HOUR_OF_DAY,
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR,
        HiveSqlDateTimeFormatter.TokenType.MINUTE,
        HiveSqlDateTimeFormatter.TokenType.SEPARATOR,
        HiveSqlDateTimeFormatter.TokenType.SECOND
    )));
    checkTokenList("y", new ArrayList<>(List.of(
        HiveSqlDateTimeFormatter.TokenType.YEAR
    )));
  }

  public void testSetPatternWithBadPatterns() {
    verifyBadPattern("e");
    verifyBadPattern("yyyy-1");
    verifyBadPattern("yyyyy");
  }

  private void verifyBadPattern(String string) {
    try {
      formatter.setPattern(string);
      fail();
    } catch (Exception e) {
      assertEquals(e.getClass().getName(), ParseException.class.getName());
    }
  }

  private void checkTokenList(String pattern, ArrayList<HiveSqlDateTimeFormatter.TokenType> expected)
      throws ParseException {
    formatter.setPattern(pattern);
    assertEquals(expected.size(), formatter.tokens.size());
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < expected.size(); i++) {
      assertEquals("Generated list of tokens not correct", expected.get(i), formatter.tokens.get(i).type);
      sb.append(formatter.tokens.get(i).string);
    }
    assertEquals("Token strings concatenated don't match original pattern string", pattern.toLowerCase(), sb.toString());
  }

  @Test
  public void testFormat() {
  }

  @Test
  public void testParse() throws ParseException {
  }
}
