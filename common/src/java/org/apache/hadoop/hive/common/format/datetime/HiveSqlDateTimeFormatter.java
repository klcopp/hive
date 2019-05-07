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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Formatter using SQL:2016 datetime patterns.
 */

public class HiveSqlDateTimeFormatter implements HiveDateTimeFormatter {

  private static final int LONGEST_TOKEN_LENGTH = 5;
  private static final int LONGEST_ACCEPTED_PATTERN = 100; // for sanity's sake
  private String pattern;
  private TimeZone timeZone;
  protected List<Token> tokens = new ArrayList<>();
  private final Map<String, TokenType> VALID_TOKENS = ImmutableMap.<String, TokenType>builder()
      .put("-", TokenType.SEPARATOR)
      .put(":", TokenType.SEPARATOR)
      .put(" ", TokenType.SEPARATOR)
      .put(".", TokenType.SEPARATOR)
      .put("/", TokenType.SEPARATOR)
      .put(";", TokenType.SEPARATOR)
      .put("\'", TokenType.SEPARATOR)
      .put(",", TokenType.SEPARATOR)

      .put("yyyy", TokenType.YEAR)
      .put("yyy", TokenType.YEAR)
      .put("yy", TokenType.YEAR)
      .put("y", TokenType.YEAR)
      .put("mm", TokenType.MONTH)
      .put("dd", TokenType.DAY_OF_MONTH)
      .put("hh", TokenType.HOUR_OF_DAY)
      .put("mi", TokenType.MINUTE)
      .put("ss", TokenType.SECOND)
      .build();

  private final Map<String, Integer> SPECIAL_LENGTHS = ImmutableMap.<String, Integer>builder()
      .put("HH12", 2)
      .put("HH24", 2)
      .put("TZM", 2)
      .put("FF", 9)
      .put("FF1", 1).put("FF2", 2).put("FF3", 2)
      .put("FF4", 2).put("FF5", 2).put("FF6", 2)
      .put("FF7", 2).put("FF8", 2).put("FF9", 2)
      .build();

  public enum TokenType {
    SEPARATOR,
    YEAR,
    MONTH,
    DAY_OF_MONTH,
    HOUR_OF_DAY,
    MINUTE,
    SECOND
  }
  
  public class Token {
    TokenType type;
    String string;
    //todo index? (update in squashseparators)

    public Token(TokenType type, String string) {
      this.type = type;
      this.string = string;
    }

    @Override public String toString() {
      return string;
    }
  }

  public HiveSqlDateTimeFormatter() {}

  /**
   * 
   */
  @Override public void setPattern(String pattern) throws ParseException {
    assert pattern.length() < LONGEST_ACCEPTED_PATTERN;
    pattern = pattern.toLowerCase().trim();

    parsePatternToTokens(pattern);

    verifyTokenTypeList();
    squashSeparators();
    this.pattern = pattern;
  }

  private void parsePatternToTokens(String pattern) throws ParseException {
    tokens.clear();

    // the substrings we will check (includes begin, does not include end)
    int begin=0, end=0;
    String candidate;
    while (begin < pattern.length()) {
      
      // if begin hasn't progressed, then something is unparseable
      if (begin != end) {
        tokens.clear();
        try {
          new SimpleDateFormat().applyPattern(pattern);
          throw new ParseException("Unable to parse format. However, it would parse with hive.use....frogmethod=false. Have you forgot to turn it off?");
          //todo this isn't true for cast statements
        } catch (IllegalArgumentException e) {
          //do nothing
        }
        throw new ParseException("Bad date/time conversion format: " + pattern);
      }
      
      //process next token
      for (int i=LONGEST_TOKEN_LENGTH; i > 0; i--) {
        end = begin + i;
        if (end > pattern.length()) {
          continue;
        }
        candidate = pattern.substring(begin, end);
        if (VALID_TOKENS.keySet().contains(candidate)) {
          tokens.add(new Token(VALID_TOKENS.get(candidate), candidate));
          begin = end;
          break;
        }
      }
    }
  }

  /**
   * Clumps of separators (e.g. "---") count as single separators ("-")
   */
  private void squashSeparators() {
    List<Token> newList = new ArrayList<>();
    Token lastToken = tokens.get(0);
    Token lastSeparatorToken = null;

    //take care of index 0
    newList.add(lastToken); // add first token no matter what
    if (lastToken.type == TokenType.SEPARATOR) {
      lastSeparatorToken = lastToken;
    }
    
    if (tokens.size() > 1) {
      Token token;
      for (int i = 1; i < tokens.size(); i++) {
        token = tokens.get(i);
        if (token.type == TokenType.SEPARATOR) { // deal with a separator
          if (lastSeparatorToken == null) {
            lastSeparatorToken = token;
            newList.add(token);
          } else if (lastToken.type == TokenType.SEPARATOR) {
            lastSeparatorToken.string += token.string;
          } else {
            newList.add(token);
          }
        } else {
          newList.add(token); // not a separator
        }
        lastToken = token;
      }
    }
    tokens = newList;
  }

  /**
   * frogmethod: errors:
   * Invalid duplication of format element
   * //https://github.infra.cloudera.com/gaborkaszab/Impala/commit/b4f0c595758c1fa23cca005c2aa378667ad0bc2b#diff-508125373d89c68468d26d960cbd0ffaR511
   * 
   * not done yet:
   * "Both year and round year are provided"
   * "Day of year provided with day or month tokenType"
   * "Multiple median indicator tokenTypes provided"
   * "Conflict between median indicator and hour tokenType"
   * "Second of day tokenType conflicts with other tokenType(s)"
   * 
   * "The input format is too long"
   * 
   * 
   * @return
   */
  
  private boolean verifyTokenTypeList() throws ParseException { // frogmethod
    StringBuilder exceptionList = new StringBuilder();
    for (TokenType tokenType: TokenType.values()) {
      if (Collections.frequency(tokens, tokenType) > 1) {
        exceptionList.append("Multiple " + tokenType.name() + " tokenTypes provided\n");
      }
    }
    if (tokens.contains(TokenType.HOUR_OF_DAY) && //todo iterate over these
        !(tokens.contains(TokenType.MINUTE) || tokens.contains(TokenType.SECOND))) { // todo or doesn't contain other hour or contains other smaller thing
      exceptionList.append("Missing hour tokenType\n");
    }
    String exceptions = exceptionList.toString();
    if (!exceptions.isEmpty()) {
      throw new ParseException(exceptions);
    }
    return true; //todo probably not necessary
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
