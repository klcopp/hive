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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
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
  private static final long MINUTES_PER_HOUR = 60;
  private static final int _50 = 50;
  private static final int NANOS_MAX_LENGTH = 9;
  public static final int AM = 0;
  public static final int PM = 1;
  private String pattern;
  // for offset hour/minute
  private TimeZone timeZone;
  protected List<Token> tokens = new ArrayList<>();

  private static final Map<String, TemporalField> VALID_TEMPORAL_TOKENS =
      ImmutableMap.<String, TemporalField>builder()
          .put("yyyy", ChronoField.YEAR).put("yyy", ChronoField.YEAR)
          .put("yy", ChronoField.YEAR).put("y", ChronoField.YEAR)
          .put("rrrr", ChronoField.YEAR).put("rr", ChronoField.YEAR)
          .put("mm", ChronoField.MONTH_OF_YEAR)
          .put("dd", ChronoField.DAY_OF_MONTH)
          .put("ddd", ChronoField.DAY_OF_YEAR)
          .put("hh", ChronoField.HOUR_OF_AMPM)
          .put("hh12", ChronoField.HOUR_OF_AMPM)
          .put("hh24", ChronoField.HOUR_OF_DAY)
          .put("mi", ChronoField.MINUTE_OF_HOUR)
          .put("ss", ChronoField.SECOND_OF_MINUTE)
          .put("sssss", ChronoField.SECOND_OF_DAY)
          .put("ff1", ChronoField.NANO_OF_SECOND).put("ff2", ChronoField.NANO_OF_SECOND)
          .put("ff3", ChronoField.NANO_OF_SECOND).put("ff4", ChronoField.NANO_OF_SECOND)
          .put("ff5", ChronoField.NANO_OF_SECOND).put("ff6", ChronoField.NANO_OF_SECOND)
          .put("ff7", ChronoField.NANO_OF_SECOND).put("ff8", ChronoField.NANO_OF_SECOND)
          .put("ff9", ChronoField.NANO_OF_SECOND).put("ff", ChronoField.NANO_OF_SECOND)
          .put("a.m.", ChronoField.AMPM_OF_DAY).put("am", ChronoField.AMPM_OF_DAY)
          .put("p.m.", ChronoField.AMPM_OF_DAY).put("pm", ChronoField.AMPM_OF_DAY)
          .build();

  private static final Map<String, TemporalUnit> VALID_TIME_ZONE_TOKENS =
      ImmutableMap.<String, TemporalUnit>builder()
          .put("tzh", ChronoUnit.HOURS).put("tzm", ChronoUnit.MINUTES).build();

  private static final List<String> VALID_ISO_8601_DELIMITERS =
      ImmutableList.of("t", "z");

  private static final List<String> VALID_SEPARATORS =
      ImmutableList.of("-", ":", " ", ".", "/", ";", "\'", ",");

  private static final Map<String, Integer> SPECIAL_LENGTHS = ImmutableMap.<String, Integer>builder()
      .put("hh12", 2).put("hh24", 2).put("tzm", 2)
      .put("ff1", 1).put("ff2", 2).put("ff3", 3).put("ff4", 4).put("ff5", 5)
      .put("ff6", 6).put("ff7", 7).put("ff8", 8).put("ff9", 9).put("ff", 9)
      .build();

  public enum TokenType {
    TEMPORAL,
    SEPARATOR,
    TIMEZONE,
    ISO_8601_DELIMITER
  }

  public class Token {
    TokenType type;
    TemporalField temporalField; // for type TEMPORAL e.g. ChronoField.YEAR
    TemporalUnit temporalUnit; // for type TIMEZONE e.g. ChronoUnit.HOURS
    String string; // pattern string, e.g. "yyy"
    int length; // length (e.g. YYY: 3, FF8: 8)

    public Token(TemporalField temporalField, String string, int length) {
      this(TokenType.TEMPORAL, temporalField, null, string, length);
    }

    public Token(TemporalUnit temporalUnit, String string, int length) {
      this(TokenType.TIMEZONE, null, temporalUnit, string, length);
    }

    public Token(TokenType tokenType, String string) {
      this(tokenType, null, null, string, string.length());
    }

    public Token(TokenType tokenType, TemporalField temporalField, TemporalUnit temporalUnit,
        String string, int length) {
      this.type = tokenType;
      this.temporalField = temporalField;
      this.temporalUnit = temporalUnit;
      this.string = string;
      this.length = length;
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(string);
      sb.append(" type: ");
      sb.append(type);
      if (temporalField != null) {
        sb.append(" temporalField: ");
        sb.append(temporalField);
      } else if (temporalUnit != null) {
        sb.append(" temporalUnit: ");
        sb.append(temporalUnit);
      }
      return sb.toString();
    }
  }

  /**
   * Parse and perhaps verify the pattern.
   */
  @Override public void setPattern(String pattern, boolean forParsing)
      throws IllegalArgumentException {
    assert pattern.length() < LONGEST_ACCEPTED_PATTERN : "The input format is too long";

    this.pattern = parsePatternToTokens(pattern);

    // throw Exception if list of tokens doesn't make sense for parsing. Formatting is less picky.
    if (forParsing) {
      verifyTokenList();
    }
  }

  /**
   * Parse pattern to list of tokens.
   */
  private String parsePatternToTokens(String pattern) throws IllegalArgumentException {
    tokens.clear();
    String originalPattern = pattern;
    pattern = pattern.toLowerCase();

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
        } else if (candidate.length() == 1 && VALID_ISO_8601_DELIMITERS.contains(candidate)) {
          lastAddedToken = new Token(TokenType.ISO_8601_DELIMITER, candidate.toUpperCase()); //todo not sure about this uppercase cast
          tokens.add(lastAddedToken);
          begin = end;
          break;
          //temporal token
        } else if (VALID_TEMPORAL_TOKENS.keySet().contains(candidate)) {
          // for AM/PM, keep original case
          if (VALID_TEMPORAL_TOKENS.get(candidate) == ChronoField.AMPM_OF_DAY) {
            int subStringEnd = begin + getTokenStringLength(candidate);
            candidate = originalPattern.substring(begin, subStringEnd);
            pattern = pattern.substring(0, begin) + candidate + pattern.substring(subStringEnd);
          }
          lastAddedToken = new Token(VALID_TEMPORAL_TOKENS.get(candidate.toLowerCase()), candidate,
              getTokenStringLength(candidate));
          tokens.add(lastAddedToken);
          begin = end;
          break;
          //time zone
        } else if (VALID_TIME_ZONE_TOKENS.keySet().contains(candidate)) {
          lastAddedToken = new Token(VALID_TIME_ZONE_TOKENS.get(candidate), candidate,
              getTokenStringLength(candidate));
          tokens.add(lastAddedToken);
          begin = end;
          break;
        }
      }
    }
    return pattern;
  }

  private int getTokenStringLength(String candidate) {
    if (SPECIAL_LENGTHS.containsKey(candidate)) {
      return SPECIAL_LENGTHS.get(candidate);
    }
    return candidate.length();
  }

  /**
   * Make sure the generated list of Tokens is parseable.
   *
   * todo
   * https://github.infra.cloudera.com/gaborkaszab/Impala/commit/b4f0c595758c1fa23cca005c2aa378667ad0bc2b#diff-508125373d89c68468d26d960cbd0ffaR511
   * not done: "Both year and round year are provided"
   */

  private void verifyTokenList() throws IllegalArgumentException {

    // create a list of tokens' temporal fields
    ArrayList<TemporalField> temporalFields = new ArrayList<>();
    ArrayList<TemporalUnit> timeZoneTemporalUnits = new ArrayList<>();
    int roundYearCount=0 ,yearCount=0;
    for (Token token : tokens) {
      if (token.temporalField != null) {
        temporalFields.add(token.temporalField);
        if (token.temporalField == ChronoField.YEAR) {
          if (token.string.startsWith("r")) {
            roundYearCount += 1;
          } else {
            yearCount += 1;
          }
        }
      } else if (token.temporalUnit != null) {
        timeZoneTemporalUnits.add(token.temporalUnit);
      }
    }

    if (roundYearCount > 0 && yearCount > 0) {
      throw new IllegalArgumentException("Invalid duplication of format element: Both year and"
          + "round year are provided");
    }
    for (TemporalField tokenType : temporalFields) {
      if (Collections.frequency(temporalFields, tokenType) > 1) {
        throw new IllegalArgumentException(
            "Invalid duplication of format element: multiple " + tokenType.toString()
                + " tokens provided.");
      }
    }
    if (temporalFields.contains(ChronoField.AMPM_OF_DAY) &&
        !(temporalFields.contains(ChronoField.HOUR_OF_DAY) ||
            temporalFields.contains(ChronoField.HOUR_OF_AMPM))) {
      throw new IllegalArgumentException("Missing hour token.");
    }
    if (temporalFields.contains(ChronoField.AMPM_OF_DAY) &&
        temporalFields.contains(ChronoField.HOUR_OF_DAY)) {
      throw new IllegalArgumentException("Conflict between median indicator and hour tokenType.");
    }
    if (temporalFields.contains(ChronoField.DAY_OF_YEAR) &&
        (temporalFields.contains(ChronoField.DAY_OF_MONTH) ||
            temporalFields.contains(ChronoField.MONTH_OF_YEAR))) {
      throw new IllegalArgumentException("Day of year provided with day or month tokenType.");
    }
    if (temporalFields.contains(ChronoField.SECOND_OF_DAY) &&
        (temporalFields.contains(ChronoField.HOUR_OF_DAY) ||
            temporalFields.contains(ChronoField.HOUR_OF_AMPM) ||
            temporalFields.contains(ChronoField.MINUTE_OF_HOUR) ||
            temporalFields.contains(ChronoField.SECOND_OF_MINUTE))) {
      throw new IllegalArgumentException(
          "Second of day tokenType conflicts with other tokenType(s).");
    }
    if (timeZoneTemporalUnits.contains(ChronoUnit.MINUTES) &&
        !timeZoneTemporalUnits.contains(ChronoUnit.HOURS)) {
      throw new IllegalArgumentException("TZM without TZH."); //todo [wf] KG's phrasing
    }
  }

  @Override public String format(Timestamp ts) throws FormatException {
    StringBuilder fullOutputSb = new StringBuilder();
    String outputString = null;
    int value;
    LocalDateTime localDateTime =
        LocalDateTime.ofEpochSecond(ts.toEpochSecond(), ts.getNanos(), ZoneOffset.UTC);
    for (Token token : tokens) {
      switch (token.type) {
      case TEMPORAL:
        try {
          value = localDateTime.get(token.temporalField);
          outputString = formatTemporal(value, token);
        } catch (DateTimeException e) {
          throw new FormatException(token.temporalField + " couldn't be obtained from"
              + "LocalDateTime " + localDateTime, e);
        }
        break;
      case TIMEZONE:
        outputString = formatTimeZone(timeZone, localDateTime, token);
        break;
      case SEPARATOR:
        outputString = token.string;
        break;
      case ISO_8601_DELIMITER:
        outputString = token.string.toUpperCase();
        break;
      }
      fullOutputSb.append(outputString);
    }
    return fullOutputSb.toString();
  }

  private String formatTemporal(int value, Token token) throws FormatException {
    String output;
    if (token.temporalField == ChronoField.AMPM_OF_DAY) {
      output = value == 0 ? "a" : "p";
      output += token.length == 2 ? "m" : ".m.";
      if (token.string.startsWith("A") || token.string.startsWith("P")) {
        output = output.toUpperCase();
      }
    } else {
      // it's a numeric value
      try {
        output = String.valueOf(value);
        output = padOrTruncateNumericTemporal(token, output);
      } catch (Exception e) { //todo which excaption??
        throw new FormatException("Value: " + value + " couldn't be cast to string.");
      }
    }
    return output;
  }

  /**
   * To match token.length, pad left with zeroes or truncate.
   */
  private String padOrTruncateNumericTemporal(Token token, String output) {
    if (output.length() < token.length) {
      output = StringUtils.leftPad(output, token.length, '0'); // pad left
    } else if (output.length() > token.length) {
      if (token.temporalField == ChronoField.NANO_OF_SECOND) {
        output = output.substring(0, token.length); // truncate right
      } else {
        output = output.substring(output.length() - token.length); // truncate left
      }
    }
    return output;
  }

  private String formatTimeZone(TimeZone timeZone, LocalDateTime localDateTime, Token token) {
    ZoneOffset offset = timeZone.toZoneId().getRules().getOffset(localDateTime);
    Duration seconds = Duration.of(offset.get(ChronoField.OFFSET_SECONDS), ChronoUnit.SECONDS);
    if (token.string.equals("tzh")) {
      long hours = seconds.toHours();
      String s = (hours >= 0) ? "+" : "-";
      s += (Math.abs(hours) < 10) ? "0" : "";
      s += String.valueOf(Math.abs(hours));
      return s;
    } else {
      long minutes = Math.abs(seconds.toMinutes() % MINUTES_PER_HOUR);
      String s = String.valueOf(minutes);
      if (s.length() == 1) {
        s = "0" + s;
      }
      return s;
    }
  }

  @Override public Timestamp parse(String fullInput) throws ParseException {
    LocalDateTime ldt = LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
    String substring;
    int index = 0;
    int value;
    int timeZoneSign = 0, timeZoneMinutes = 0;

    for (Token token : tokens) {
      switch (token.type) {
      case TEMPORAL:
        substring = getNextSubstring(fullInput, index, token); // e.g. yy-m -> yy
        value = parseTemporal(substring, token); // e.g. 18->2018, July->07
        try {
          ldt = ldt.with(token.temporalField, value);
        } catch (DateTimeException e){
          throw new ParseException("Value " + value + " not valid for token " + token.toString());
        }
        index += substring.length();
        break;
      case TIMEZONE:
        substring = getNextSubstring(fullInput, index, token);
        value = Integer.valueOf(substring);
        if (token.temporalUnit == ChronoUnit.HOURS) {
          ldt = ldt.minus(value, token.temporalUnit);
          //save sign for time zone minutes
          timeZoneSign = "-".equals(fullInput.substring(index, index + 1)) ? -1 : 1;
        } else {
          timeZoneMinutes = value;
        }
        index += substring.length();
        break;
      case SEPARATOR:
        index = parseSeparator(fullInput, index, token);
        break;
      case ISO_8601_DELIMITER:
        substring = fullInput.substring(index, index + 1);
        if (token.string.equalsIgnoreCase(substring)) {
          index++;
        } else {
          throw new ParseException("Missing ISO 8601 delimiter " + token.string.toUpperCase());
        }
      }
    }
    // time zone minutes -- process here because sign depends on tzh sign
    ldt = ldt.minus(timeZoneSign * timeZoneMinutes, ChronoUnit.MINUTES);

    // deal with potential TimestampLocalTZ time zone ID at end of string
    ZoneId zoneId = getZoneId(fullInput, index);

    return Timestamp
        .ofEpochSecond(ldt.toEpochSecond(zoneId.getRules().getOffset(ldt)), ldt.getNano());
  }

  /**
   * Return the next substring to parse. Length is expected token.length, but a separator can cut
   * the substring short. (e.g. if the token pattern is "YYYY" we expect the next 4 characters to
   * be 4 numbers. However, if it is "976/" then we return "976" because a separator cuts it short)
   */
  private String getNextSubstring(String s, int begin, Token token) {
    int end = begin + token.length;
    if (end > s.length()) {
      end = s.length();
    }
    s = s.substring(begin, end);
    for (String sep : VALID_SEPARATORS) {
      if (s.contains(sep) &&
          // "." is a separator but e.g. "A.M." is a token, so ignore "."s if necessary
          !(sep.equals(".") && token.type == TokenType.TEMPORAL &&
              token.temporalField == ChronoField.AMPM_OF_DAY & token.length == 4) &&
          !(sep.equals("-") && token.type == TokenType.TIMEZONE)) {
        s = s.substring(0, s.indexOf(sep));
      }
    }
    return s;
  }

  /**
   * Get the integer value of a temporal substring.
   */
  private int parseTemporal(String substring, Token token) throws ParseException {
    // exceptions to the rule
    if (token.temporalField == ChronoField.AMPM_OF_DAY) {
      return substring.toLowerCase().startsWith("a") ? AM : PM;

    } else if (token.temporalField == ChronoField.YEAR) {
      String currentYearString = String.valueOf(LocalDateTime.now().getYear());
      //deal with round years
      if (token.string.startsWith("r") && substring.length() == 2) {
        int currFirst2Digits = Integer.valueOf(currentYearString.substring(0, 2));
        int currLast2Digits = Integer.valueOf(currentYearString.substring(2));
        int valLast2Digits = Integer.valueOf(substring);
        if (valLast2Digits < _50 && currLast2Digits >= _50) {
          currFirst2Digits += 1;
        } else if (valLast2Digits >= _50 && currLast2Digits < _50) {
          currFirst2Digits -= 1;
        }
        substring = String.valueOf(currFirst2Digits) + substring;
      } else { // fill in prefix digits with current date
        substring = currentYearString.substring(0, 4 - substring.length()) + substring;
      }

    } else if (token.temporalField == ChronoField.NANO_OF_SECOND) {
      int i = Integer.min(token.length, substring.length());
      substring += StringUtils.repeat("0", NANOS_MAX_LENGTH - i);
    }

    // the rule
    try {
      return Integer.valueOf(substring);
    } catch (NumberFormatException e) {
      throw new ParseException(
          "Couldn't parse substring " + substring + " with token " + token + " to int. Pattern is "
              + pattern, e);
    }
  }

  /**
   * Parse the next separator(s). At least one separator character is expected. Separator
   * characters are interchangeable.
   *
   * Caveat: If the last separator character in the separator substring is "-" and is immediately
   *     followed by a time zone hour (tzh) token, it's a negative sign and not counted as a
   *     separator, UNLESS this is the only separator character in the separator substring (in
   *     which case it is not counted as the negative sign).
   *
   * @throws ParseException if separator is missing
   */
  private int parseSeparator(String fullInput, int index, Token token) throws ParseException {
    int separatorsFound = 0;
    int begin = index;

    while (index < fullInput.length() &&
        VALID_SEPARATORS.contains(fullInput.substring(index, index + 1))) {
      if (!isLastCharacterOfSeparator(index, fullInput) || !(nextTokenIs("tzh", token))
          || separatorsFound == 0) {
        separatorsFound++;
      }
      index++;
    }

    if (separatorsFound == 0) {
      throw new ParseException("Missing separator at index " + index);
    }
    return begin + separatorsFound;
  }

  /**
   * Is the next character a separator?
   */
  private boolean isLastCharacterOfSeparator(int index, String string) {
    return !VALID_SEPARATORS.contains(string.substring(index + 1, index + 2));
  }

  /**
   * Does the temporalUnit/temporalField of the next token match the pattern's?
   */
  private boolean nextTokenIs(String pattern, Token currentToken) {
    // make sure currentToken isn't the last one
    if (tokens.indexOf(currentToken) == tokens.size() - 1) {
      return false;
    }
    Token nextToken = tokens.get(tokens.indexOf(currentToken) + 1);
    pattern = pattern.toLowerCase();
    return (VALID_TIME_ZONE_TOKENS.containsKey(pattern)
        && VALID_TIME_ZONE_TOKENS.get(pattern) == nextToken.temporalUnit
        || VALID_TEMPORAL_TOKENS.containsKey(pattern)
        && VALID_TEMPORAL_TOKENS.get(pattern) == nextToken.temporalField);
  }

  /**
   * Deal with potential TimestampLocalTZ time zone ID at end of string.
   */
  private ZoneId getZoneId(String fullInput, int index) throws ParseException {
    String substring;
    ZoneId zoneId = ZoneOffset.UTC;
    if (index != fullInput.length()) { // frogmethod todo [waiting for] ignore excess input or throw error?
      substring = fullInput.substring(index).trim();
      try {
        zoneId = ZoneId.of(substring);
      } catch (DateTimeException e) {
        throw new ParseException(
            "Can't parse substring at end of input: " + substring + " from string: " + fullInput
                + " with pattern " + pattern, e);
      }
    }
    return zoneId;
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
