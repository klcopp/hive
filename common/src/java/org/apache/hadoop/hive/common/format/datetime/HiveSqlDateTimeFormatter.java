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

  public static final int LONGEST_TOKEN_LENGTH = 5;
  private String pattern;
  private TimeZone timeZone;
  private List<Token> tokens = new ArrayList<>();

  public HiveSqlDateTimeFormatter() {}

  

  private enum Token {
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
      .build();
  
  

  @Override public void setPattern(String pattern) {
    this.pattern = pattern.toLowerCase().trim();

    tokens.clear();
    int curIndex = 0, end;
    String candidate;
    while (curIndex < pattern.length() - 1) {
      for (int i=1; i < LONGEST_TOKEN_LENGTH; i++) { // todo check the range
        end = curIndex + i; //range: 0, 0-1 ... 0-4
        candidate = pattern.substring(curIndex, end); //todo init in loo;ps
        if (tokenMap.keySet().contains(candidate)) {
          tokens.add(tokenMap.get(candidate));
          curIndex = end;
          break;
        }
      }
      
    }
//    find first separator (or beginning of string) in formatstring. save next separator existence and (length == 1)
//    if not at formatstring end:
//    parse next token . this means:
//    for next substring (length: longesttokenlength to 1):
//    if substring in token map:
//    next token = (token value of substring)
    
    
    
  }

  @Override public String getPattern() {
    return pattern;
  }

  @Override public String format(Timestamp ts) {
    //TODO replace with actual implementation:
    HiveDateTimeFormatter formatter = new HiveSimpleDateFormatter();
    formatter.setPattern(pattern);
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
