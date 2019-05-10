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
import org.apache.hadoop.hive.common.type.Timestamp;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.chrono.ChronoLocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
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
  private TimeZone timeZone;
  protected List<Token> tokens = new ArrayList<>();

  private final Map<String, TemporalField> VALID_TOKENS =
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

  private final List<String> VALID_SEPARATORS =
          ImmutableList.of("-", ":", " ", ".", "/", ";", "\'", ",");

  private final Map<String, Integer> SPECIAL_LENGTHS = ImmutableMap.<String, Integer>builder()
      .put("hh12", 2)
      .put("hh24", 2)
      .put("tzm", 2)
      .put("ff", 9)
      .put("ff1", 1).put("ff2", 2).put("ff3", 3)
      .put("ff4", 4).put("ff5", 5).put("ff6", 6)
      .put("ff7", 7).put("ff8", 8).put("ff9", 9)
      .build();
  private DateTimeFormatter dateTimeFormatter;

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
      return string + " type: " + type;
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
    createDateTimeFormat();
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
        } else if (VALID_TOKENS.keySet().contains(candidate)) {
          lastAddedToken = new Token(VALID_TOKENS.get(candidate), candidate, getCandidateLength(candidate));
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

  @Override public String getPattern() {
    return pattern;
  }

  private void createDateTimeFormat() {
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();

    for (Token token: tokens) {
      if (token.type == TokenType.SEPARATOR) {
        builder.appendLiteral(token.string);
      } else {
        builder.appendValueReduced(token.temporalField, 1, token.length, LocalDate.now()); // frogmethod: signstyle : NOT_NEGATIVE? (only important for parsing)
        builder.parseDefaulting(token.temporalField, token.temporalField.range().getMinimum());
      }
      builder.optionalStart().appendLiteral(' ').optionalEnd();
    }

//    builder.parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
//            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
//            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0);
    dateTimeFormatter = builder.toFormatter().withResolverStyle(ResolverStyle.LENIENT);
  }

  @Override public String format(Timestamp ts) {
    LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(ts.toEpochSecond(), ts.getNanos(), ZoneOffset.UTC);
    try {
      return localDateTime.format(dateTimeFormatter);
    } catch (DateTimeException e) {
      return null; //todo frogmethod FormatException? Or DateTimeException?
    }
  }

  @Override public Timestamp parse(String string) throws ParseException {
    try {
      LocalDateTime ldt = LocalDateTime.parse(string, dateTimeFormatter);
      return Timestamp.ofEpochSecond(ldt.toEpochSecond(ZoneOffset.UTC), ldt.getNano());
    } catch (DateTimeException e) {
//      try {
//        LocalDate ld = LocalDate.parse(string, dateTimeFormatter);
//        LocalDateTime ldt = ld.atStartOfDay();
//        return Timestamp.ofEpochSecond(ldt.toEpochSecond(ZoneOffset.UTC), ldt.getNano());
//      } catch (DateTimeException e2) {
        throw new ParseException("Could not parse string " + string + " with SQL:2016 format " +
                "pattern " + pattern, e); //frogmethod e or e2 could be the cause
      }
//    }

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
