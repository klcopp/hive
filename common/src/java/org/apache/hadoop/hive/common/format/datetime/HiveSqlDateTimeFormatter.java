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
      .put("hh", TokenType.HOUR_IN_HALF_DAY)
      .put("mi", TokenType.MINUTE)
      .put("ss", TokenType.SECOND)
      .build();

  private final Map<String, Integer> SPECIAL_LENGTHS = ImmutableMap.<String, Integer>builder()
      .put("hh12", 2)
      .put("hh24", 2)
      .put("tzm", 2)
      .put("ff", 9)
      .put("ff1", 1).put("ff2", 2).put("ff3", 3)
      .put("ff4", 4).put("ff5", 5).put("ff6", 6)
      .put("ff7", 7).put("ff8", 8).put("ff9", 9)
      .build();

  public enum TokenType {
    SEPARATOR,
    YEAR,
    MONTH,
    DAY_OF_MONTH,
    HOUR_IN_HALF_DAY,
    MINUTE,
    SECOND
  }
  
  public class Token {
    TokenType type;
    String string; // pattern string
    int length; // length of output (e.g. YYY: 3, FF8: 8)

    public Token(TokenType type, String string, int length) {
      this.type = type;
      this.string = string;
      this.length = length;
    }

    @Override public String toString() {
      return string + " type: " + type;
    }
  }

  public HiveSqlDateTimeFormatter() {}

  /**
   * Parses the pattern.
   */
  @Override public void setPattern(String pattern) throws ParseException {
    assert pattern.length() < LONGEST_ACCEPTED_PATTERN : "The input format is too long";
    pattern = pattern.toLowerCase();

    parsePatternToTokens(pattern);

    verifyTokenList(); // throw Exception if list of tokens doesn't make sense
    this.pattern = pattern;
  }

  /**
   * Updates list of tokens 
   */
  private void parsePatternToTokens(String pattern) throws ParseException {
    tokens.clear();

    // the substrings we will check (includes begin, does not include end)
    int begin=0, end=0;
    String candidate;
    Token lastAddedToken = null;

    while (begin < pattern.length()) {
      
      // if begin hasn't progressed, then something is unparseable
      if (begin != end) {
        tokens.clear();

//        try {
//          new SimpleDateFormat().applyPattern(pattern);
//          throw new ParseException("Unable to parse format. However, it would parse with hive.use....frogmethod=false. Have you forgot to turn it off?");
//          //todo this isn't true for cast statements
//        } catch (IllegalArgumentException e) {
//          //do nothing
//        }
        throw new ParseException("Bad date/time conversion format: " + pattern);
      }

      //process next token
      for (int i = LONGEST_TOKEN_LENGTH; i > 0; i--) {
        end = begin + i;
        if (end > pattern.length()) {
          continue;
        }
        candidate = pattern.substring(begin, end);
        if (VALID_TOKENS.keySet().contains(candidate)) {
          // if it's a separator, then clump it with immediately preceding separators (e.g. "---"
          // counts as one separator). Otherwise add token to the list.
          if (VALID_TOKENS.get(candidate) == TokenType.SEPARATOR &&
              lastAddedToken != null &&
              lastAddedToken.type == TokenType.SEPARATOR) {
            lastAddedToken.string += candidate;
            lastAddedToken.length += candidate.length();
          } else {
            lastAddedToken =
                new Token(VALID_TOKENS.get(candidate), candidate, getCandidateLength(candidate));
            tokens.add(lastAddedToken);
          }
          begin = end;
          break;
        }
        
      }
    }
  }

  private int getCandidateLength(String candidate) {
    if (SPECIAL_LENGTHS.containsKey(candidate)) {
      return SPECIAL_LENGTHS.get(candidate);
    }
    return candidate.length();
  }

  /**
   * frogmethod: errors:
   * https://github.infra.cloudera.com/gaborkaszab/Impala/commit/b4f0c595758c1fa23cca005c2aa378667ad0bc2b#diff-508125373d89c68468d26d960cbd0ffaR511
   * 
   * not done yet:
   * "Missing hour token"(when meridian indicator is present but any type of hour is absent)
   * "Both year and round year are provided"
   * "Day of year provided with day or month tokenType"
   * "Multiple median indicator tokenTypes provided"
   * "Conflict between median indicator and hour tokenType"
   * "Second of day tokenType conflicts with other tokenType(s)"
   */
  
  private void verifyTokenList() throws ParseException { // frogmethod

    // create a list of token types
    ArrayList<TokenType> tokenTypes = new ArrayList<>();
    for (Token token : tokens) {
      tokenTypes.add(token.type);
    }

    // check for bad combinations of token types
    StringBuilder exceptionList = new StringBuilder();
    for (TokenType tokenType: TokenType.values()) {
      if (Collections.frequency(tokenTypes, tokenType) > 1 &&
          tokenType != TokenType.SEPARATOR) {
        exceptionList.append("Invalid duplication of format element: multiple ");
        exceptionList.append(tokenType.name());
        exceptionList.append(" tokens provided\n");
      }
    }
    //todo...

    String exceptions = exceptionList.toString();
    if (!exceptions.isEmpty()) {
      throw new ParseException(exceptions);
    }
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
