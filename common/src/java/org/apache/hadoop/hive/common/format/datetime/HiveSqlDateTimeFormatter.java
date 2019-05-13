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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;
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
  // for offset hour/minute
  private TimeZone timeZone;
  protected List<Token> tokens = new ArrayList<>();

  private static final Map<String, TemporalField> VALID_TEMPORAL_TOKENS =
          ImmutableMap.<String, TemporalField>builder()
                  .put("yyyy", ChronoField.YEAR)
                  .put("yyy", ChronoField.YEAR)
                  .put("yy", ChronoField.YEAR)
                  .put("y", ChronoField.YEAR)
                  .put("mm", ChronoField.MONTH_OF_YEAR)
                  .put("dd", ChronoField.DAY_OF_MONTH)
                  .put("hh24", ChronoField.HOUR_OF_DAY)
                  .put("mi", ChronoField.MINUTE_OF_HOUR)
                  .put("ss", ChronoField.SECOND_OF_MINUTE)
                  .build();

  private static final List<String> VALID_SEPARATORS =
          ImmutableList.of("-", ":", " ", ".", "/", ";", "\'", ",");

  private static final Map<String, Integer> SPECIAL_LENGTHS = ImmutableMap.<String, Integer>builder()
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
    TEMPORAL
    //TEXT etc.
  }

  public class Token {
    TokenType type;
    TemporalField temporalField; // e.g. ChronoField.YEAR, ISOFields.WEEK_BASED_YEAR
    String string; // pattern string, e.g. "yyy"
    int length; // length (e.g. YYY: 3, FF8: 8)

    //for temporal objects (years, months...)
    public Token(TemporalField temporalField, String string, int length) {
      this.type = TokenType.TEMPORAL;
      this.temporalField = temporalField;
      this.string = string;
      this.length = length;
    }

    //for other objects (text, separators...)
    public Token(TokenType tokenType, String string) {
      this.type = tokenType;
      this.temporalField = null;
      this.string = string;
      this.length = string.length();
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(string);
      sb.append(" type: ");
      sb.append(type);
      if (temporalField != null) {
        sb.append(" temporalField: ");
        sb.append(temporalField);
      }
      return sb.toString();
    }
  }

  public HiveSqlDateTimeFormatter() {}

  /**
   * Parses the pattern.
   */
  @Override public void setPattern(String pattern, boolean forParsing)
      throws IllegalArgumentException {
    assert pattern.length() < LONGEST_ACCEPTED_PATTERN : "The input format is too long";
    pattern = pattern.toLowerCase(); //todo save original pattern for AM/PM

    parsePatternToTokens(pattern);

    // throw Exception if list of tokens doesn't make sense for parsing. Formatting is less picky.
    if (forParsing) {
      verifyTokenList();
    }
    this.pattern = pattern;
  }

  /**
   * Updates list of tokens
   */
  private void parsePatternToTokens(String pattern) throws IllegalArgumentException {
    tokens.clear();

    // indexes of the substring we will check (includes begin, does not include end)
    int begin=0, end=0;
    String candidate;
    Token lastAddedToken = null;

    while (begin < pattern.length()) {

      // if begin hasn't progressed, then something is unparseable
      if (begin != end) {
        tokens.clear();
        throw new IllegalArgumentException("Bad date/time conversion format: " + pattern);
      }

      //process next token: start with substring
      for (int i = LONGEST_TOKEN_LENGTH; i > 0; i--) {
        end = begin + i;
        if (end > pattern.length()) { // don't go past the end of the pattern string
          continue;
        }
        candidate = pattern.substring(begin, end);
        // if it's a separator, then clump it with immediately preceding separators (e.g. "---"
        // counts as one separator).
        if (candidate.length() == 1 && VALID_SEPARATORS.contains(candidate)) {
          if (lastAddedToken != null && lastAddedToken.type == TokenType.SEPARATOR) {
            lastAddedToken.string += candidate;
            lastAddedToken.length += 1;
          } else {
            lastAddedToken = new Token(TokenType.SEPARATOR, candidate);
            tokens.add(lastAddedToken);
          }
          begin = end;
          break;
        // Otherwise add token to the list.
        } else if (VALID_TEMPORAL_TOKENS.keySet().contains(candidate)) {
          lastAddedToken = new Token(VALID_TEMPORAL_TOKENS.get(candidate), candidate, getCandidateLength(candidate));
          tokens.add(lastAddedToken);
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
   * Make sure the generated list of Tokens is valid.
   *
   * frogmethod: errors:
   * https://github.infra.cloudera.com/gaborkaszab/Impala/commit/b4f0c595758c1fa23cca005c2aa378667ad0bc2b#diff-508125373d89c68468d26d960cbd0ffaR511
   * todo
   * not done yet:
   * "Missing hour token"(when meridian indicator is present but any type of hour is absent)
   * "Both year and round year are provided"
   * "Day of year provided with day or month tokenType"
   * "Multiple median indicator tokenTypes provided"
   * "Conflict between median indicator and hour tokenType"
   * "Second of day tokenType conflicts with other tokenType(s)"
   */

  private void verifyTokenList() throws IllegalArgumentException {

    // create a list of tokens' temporal fields
    ArrayList<TemporalField> tokenTypes = new ArrayList<>();
    for (Token token : tokens) {
      if (token.temporalField != null) {
        tokenTypes.add(token.temporalField);
      }
    }

    // check for bad combinations of temporal fields
    StringBuilder exceptionList = new StringBuilder();

    //example: No duplicate anything todo I think only no duplicate years
    for (TemporalField tokenType: tokenTypes) {
      if (Collections.frequency(tokenTypes, tokenType) > 1) {
        exceptionList.append("Invalid duplication of format element: multiple ");
        exceptionList.append(tokenType.toString());
        exceptionList.append(" tokens provided\n");
      }
    }
    //etc. (don't forget the newline at the end of the errors)

    String exceptions = exceptionList.toString();
    if (!exceptions.isEmpty()) {
      throw new IllegalArgumentException(exceptions);
    }
  }

  @Override public String format(Timestamp ts) {

    StringBuilder sb = new StringBuilder();
    String output = null; //todo rename
    LocalDateTime ldt =
        LocalDateTime.ofEpochSecond(ts.toEpochSecond(), ts.getNanos(), ZoneOffset.UTC);

    for (Token token : tokens) {
      switch (token.type) {
      case TEMPORAL:
        if (token.temporalField == ChronoField.MONTH_OF_YEAR) {
          if (token.string.equals("mm")) {
            output = String.valueOf(ldt.get(ChronoField.MONTH_OF_YEAR));
          }
        } else {
          // ss, mi, hh24, dd, y..., r...
          output = String.valueOf(ldt.get(token.temporalField)); //todo catch exceptions for get()
        }

        if (output.length() < token.length) { // todo AND it's numeric
          output = StringUtils.leftPad(output, token.length, '0');
        } else if (output.length() > token.length) {
          output = output.substring(output.length()-token.length);
        }
        break;
      case SEPARATOR:
        output = token.string;
        break;
      }
      sb.append(output);
    }
    return sb.toString();
  }

  @Override public Timestamp parse(String string) throws ParseException {

    LocalDateTime ldt = LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
    String substring;
    int val;

    int begin=0, end;
    for (Token token : tokens) {
      switch (token.type) {
      case TEMPORAL:
        end = begin + token.length;
        substring = getNextSubstring(string, begin, end); // e.g. yy-m -> yy
        val = getFinalForm(substring, token); // e.g. 18->2018, July->07
        ldt = ldt.with(token.temporalField, val);
        begin = end;
        break;
      case SEPARATOR:
        begin += token.length;
        break;
      }
    }

    // deal with potential TimestampLocalTZ time zone ID at end of string
    ZoneId zoneId = ZoneOffset.UTC;
    if (begin != string.length()) {
      substring = string.substring(begin).trim();
      try {
        zoneId = ZoneId.of(substring);
      } catch (DateTimeException e) {
        throw new ParseException("Can't parse substring " + substring + " from string " + string + " with pattern " + pattern, e);
      }
    }
    return Timestamp.ofEpochSecond(ldt.toEpochSecond(zoneId.getRules().getOffset(ldt)), ldt.getNano());
  }

  /**
   * @return the substring between begin and the next separator or end, whichever comes first.
   */
  private String getNextSubstring(String s, int begin, int end) {
    s = s.substring(begin, end);
    for (String sep : VALID_SEPARATORS) {
      if (s.contains(sep)) {
        s = s.substring(0, s.indexOf(sep));
      }
    }
    return s;
  }

  /**
   * todo NumberFormatException
   * @param substring
   * @param token
   * @return
   */
  private int getFinalForm(String substring, Token token) throws ParseException {
    return Integer.valueOf(substring);
  }

  @Override public String getPattern() {
    return pattern;
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
