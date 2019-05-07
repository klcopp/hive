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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Formatter using SQL:2016 datetime patterns.
 */

public class HiveSqlDateTimeFormatter implements HiveDateTimeFormatter {

  private static final int LONGEST_TOKEN_LENGTH = 5;
  private String pattern;
  private TimeZone timeZone;
  // protected for testing
  protected List<Token> tokens = new ArrayList<>();

  public HiveSqlDateTimeFormatter() {}

  public enum Token {
    SEPARATOR,
    YEAR,
    MONTH,
    DAY_OF_MONTH,
    HOUR_OF_DAY,
    MINUTE,
    SECOND
  }
  
  private enum Separators {
    //frogmethod not sure what this is for
  }
//  
//  private final Map<Token, Set<String>> tokenMap1 = ImmutableMap.<Token, Set<String>>builder()
//      .put(Token.YEAR, ImmutableSet.of("yyyy", "yyy", "yy", "y"))
//      .put(Token.MONTH, ImmutableSet.of("mm"))
//      .put(Token.DAY_OF_MONTH, ImmutableSet.of("dd"))
//      .put(Token.HOUR_OF_DAY, ImmutableSet.of("hh"))
//      .put(Token.MINUTE, ImmutableSet.of("mi"))
//      .put(Token.SECOND, ImmutableSet.of("ss"))
//      .build();

  private final Map<String, Token> tokenMap = ImmutableMap.<String, Token>builder()
      .put("yyyy", Token.YEAR)
      .put("yyy", Token.YEAR)
      .put("yy", Token.YEAR)
      .put("y", Token.YEAR)
      .put("mm", Token.MONTH)
      .put("dd", Token.DAY_OF_MONTH)
      .put("hh", Token.HOUR_OF_DAY)
      .put("mi", Token.MINUTE)
      .put("ss", Token.SECOND)
      .put("-", Token.SEPARATOR)
      .put(":", Token.SEPARATOR)
      .put(" ", Token.SEPARATOR)
      .build();

  @Override public void setPattern(String pattern) throws ParseException {
    pattern = pattern.toLowerCase().trim();
    this.pattern = pattern;

    tokens.clear();

    // the substrings we will check (includes begin, does not include end)
    int begin=0, end=0;
    String candidate;
    while (begin < pattern.length()) {
      
      // if begin hasn't progressed, then something is unparseable
      if (begin != end) {
        throw new ParseException("Bad date/time conversion format: " + pattern);
      }
      
      for (int i=LONGEST_TOKEN_LENGTH; i > 0; i--) {
        end = begin + i;
        if (end > pattern.length()) {
          continue;
        }
        candidate = pattern.substring(begin, end);
        if (tokenMap.keySet().contains(candidate)) {
          tokens.add(tokenMap.get(candidate));
          begin = end;
          break;
        }
      }
    }
    
    verifyTokenList();
  }

  /**
   * frogmethod: errors:
   * Invalid duplication of format element
   * //https://github.infra.cloudera.com/gaborkaszab/Impala/commit/b4f0c595758c1fa23cca005c2aa378667ad0bc2b#diff-508125373d89c68468d26d960cbd0ffaR511
   * 
   * 
   * "Multiple year token provided"
   * "Both year and round year are provided"
   * "Day of year provided with day or month token"
   * "Multiple hour tokens provided"
   * "Multiple median indicator tokens provided"
   * "Conflict between median indicator and hour token"
   * "Missing hour token"
   * "Second of day token conflicts with other token(s)"
   * 
   * "The input format is too long"
   * @return
   */
  
  private boolean verifyTokenList() throws ParseException { // frogmethod
    // no duplicates except SEPARATOR?
    return true;
  }

  @Override public String getPattern() {
    return pattern;
  }

  @Override public String format(Timestamp ts) {
    //TODO replace with actual implementation:
    HiveDateTimeFormatter formatter = new HiveSimpleDateFormatter();
    try {
      formatter.setPattern(pattern);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    if (timeZone != null) formatter.setTimeZone(timeZone);
    else formatter.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
    return formatter.format(ts);
  }

  @Override public Timestamp parse(String string) throws ParseException {
    //TODO replace with actual implementation:
    // todo should be able to remove the time zone (city) from tstzs; if it doesn't then deal with
    // it in TimestampTZUtil#parseOrNull(java.lang.String, java.time.ZoneId, 
    // org.apache.hadoop.hive.common.format.datetime.HiveDateTimeFormatter)

    HiveDateTimeFormatter formatter = new HiveSimpleDateFormatter();
    formatter.setPattern(pattern);
    if (timeZone != null) formatter.setTimeZone(timeZone);
    else formatter.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
    try {
      return formatter.parse(string);
    } catch (Exception e) {
      throw new ParseException(e);
    }
  }

  @Override public void setTimeZone(TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  // unused methods
  @Override public void setFormatter(DateTimeFormatter dateTimeFormatter)
      throws WrongFormatterException {
    throw new WrongFormatterException("HiveSqlDateTimeFormatter is not a wrapper for "
        + "java.time.format.DateTimeFormatter, use HiveJavaDateTimeFormatter instead.");
  }
  @Override public void setFormatter(SimpleDateFormat simpleDateFormat)
      throws WrongFormatterException {
    throw new WrongFormatterException("HiveSqlDateTimeFormatter is not a wrapper for "
        + "java.text.SimpleDateFormat, use HiveSimpleDateFormatter instead.");
  }
}
