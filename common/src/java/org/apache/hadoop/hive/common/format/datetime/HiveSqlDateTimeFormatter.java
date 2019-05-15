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
    TIMEZONE
    //TEXT etc.
  }

  public class Token {
    TokenType type;
    TemporalField temporalField; // for type TEMPORAL e.g. ChronoField.YEAR
    TemporalUnit temporalUnit; // for type TIMEZONE e.g. ChronoUnit.HOURS
    String string; // pattern string, e.g. "yyy"
    int length; // length (e.g. YYY: 3, FF8: 8)
    
    public Token(TemporalField temporalField, String string, int length) {
      this.type = TokenType.TEMPORAL;
      this.temporalField = temporalField;
      this.string = string;
      this.length = length;
    }

    public Token(TemporalUnit temporalUnit, String string, int length) {
      this.type = TokenType.TIMEZONE;
      this.temporalUnit = temporalUnit;
      this.string = string;
      this.length = length;
    }

    public Token(TokenType tokenType, String string) {
      this.type = tokenType;
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

    parsePatternToTokens(pattern);

    // throw Exception if list of tokens doesn't make sense for parsing. Formatting is less picky.
    if (forParsing) {
      verifyTokenList();
    }
    this.pattern = pattern; // todo frogmethod pointless??? or is the og pattern good?
  }

  /**
   * Updates list of tokens
   */
  private void parsePatternToTokens(String pattern) throws IllegalArgumentException {
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
        // Otherwise add token to the list.
        } else if (VALID_TEMPORAL_TOKENS.keySet().contains(candidate)) {
          // AM/PM keep original case:
          if (VALID_TEMPORAL_TOKENS.get(candidate) == ChronoField.AMPM_OF_DAY) {
            int subStringEnd = begin + getCandidateLength(candidate);
            candidate = originalPattern.substring(begin, subStringEnd);
            pattern = pattern.substring(0, begin) + candidate + pattern.substring(subStringEnd);
          }
          lastAddedToken = new Token(VALID_TEMPORAL_TOKENS.get(candidate.toLowerCase()), candidate,
              getCandidateLength(candidate));
          tokens.add(lastAddedToken);
          begin = end;
          break;
        } else if (VALID_TIME_ZONE_TOKENS.keySet().contains(candidate)) {
          lastAddedToken = new Token(VALID_TIME_ZONE_TOKENS.get(candidate), candidate,
              getCandidateLength(candidate));
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
   * "Both year and round year are provided" //todo ?
   */

  private void verifyTokenList() throws IllegalArgumentException {

    // create a list of tokens' temporal fields
    ArrayList<TemporalField> temporalFields = new ArrayList<>();
    ArrayList<TemporalUnit> timeZoneTemporalUnits = new ArrayList<>();
//    int rCount=0 ,yCount=0; //todo [wf] rounded year + plain year
    for (Token token : tokens) {
      if (token.temporalField != null) {
        temporalFields.add(token.temporalField);
//        if (token.temporalField == ChronoField.YEAR) {
//          if (token.string.startsWith("r")) {
//            rCount += 1;
//          } else {
//            yCount += 1;
//          }
//        }
      } else if (token.temporalUnit != null) {
        timeZoneTemporalUnits.add(token.temporalUnit);
      }
    }

    // check for bad combinations of temporal fields
    StringBuilder exceptionList = new StringBuilder();

    //No duplicate anything
    for (TemporalField tokenType: temporalFields) {
      if (Collections.frequency(temporalFields, tokenType) > 1) {
        exceptionList.append("Invalid duplication of format element: multiple ");
        exceptionList.append(tokenType.toString());
        exceptionList.append(" tokens provided\n");
      }
    }
    if (temporalFields.contains(ChronoField.AMPM_OF_DAY) && !(
        temporalFields.contains(ChronoField.HOUR_OF_DAY) || temporalFields
            .contains(ChronoField.HOUR_OF_AMPM))) {
      exceptionList.append("Missing hour token\n");
    }
    if (temporalFields.contains(ChronoField.AMPM_OF_DAY) &&
        temporalFields.contains(ChronoField.HOUR_OF_DAY)) {
      exceptionList.append("Conflict between median indicator and hour tokenType\n");
    }
    if (temporalFields.contains(ChronoField.DAY_OF_YEAR) &&
        (temporalFields.contains(ChronoField.DAY_OF_MONTH) ||
            temporalFields.contains(ChronoField.MONTH_OF_YEAR))) {
      exceptionList.append("Day of year provided with day or month tokenType\n");
    }
    if (temporalFields.contains(ChronoField.SECOND_OF_DAY) &&
        (temporalFields.contains(ChronoField.HOUR_OF_DAY) ||
            temporalFields.contains(ChronoField.HOUR_OF_AMPM) ||
            temporalFields.contains(ChronoField.MINUTE_OF_HOUR) ||
            temporalFields.contains(ChronoField.SECOND_OF_MINUTE))) {
      exceptionList.append("Second of day tokenType conflicts with other tokenType(s)\n");
    }
    if (timeZoneTemporalUnits.contains(ChronoUnit.MINUTES) &&
        !timeZoneTemporalUnits.contains(ChronoUnit.HOURS)) {
      exceptionList.append("TZM without TZH"); //todo [wf] Gábor's fix
    }

    String exceptions = exceptionList.toString();
    if (!exceptions.isEmpty()) {
      throw new IllegalArgumentException(exceptions);
    }
  }

  @Override public String format(Timestamp ts) {

    StringBuilder sb = new StringBuilder(); //todo rename
    String output = null; //todo rename
    int value;
    LocalDateTime localDateTime =
        LocalDateTime.ofEpochSecond(ts.toEpochSecond(), ts.getNanos(), ZoneOffset.UTC);
    for (Token token : tokens) {
      switch (token.type) {
      case TEMPORAL:
        value = localDateTime.get(token.temporalField); //todo catch exception
        output = formatTemporal(value, token);
        break;
      case SEPARATOR:
        output = token.string;
        break;
      case TIMEZONE:
        output = formatTimeZone(timeZone, localDateTime, token);
        break;
      }
      sb.append(output);
    }
    return sb.toString();
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

  private String formatTemporal(int value, Token token) { //todo throws formatexception??
    String output;
    if (token.temporalField == ChronoField.AMPM_OF_DAY) {
      output = value == 0 ? "a" : "p";
      output += token.length == 2 ? "m" : ".m.";
      if (token.string.startsWith("A") || token.string.startsWith("P")) {
        output = output.toUpperCase();
      }
      
//    } else if () {
    } else {
      // it's a numeric value, so pad with 0's or crop first n digits if necessary
      output = String.valueOf(value);
      if (output.length() < token.length) {
        output = StringUtils.leftPad(output, token.length, '0');
      } else if (output.length() > token.length) {
        if (token.temporalField == ChronoField.NANO_OF_SECOND) {
          output = output.substring(0, token.length);
        } else {
          output = output.substring(output.length() - token.length);
        }
      }
    }
    return output;
  }

  @Override public Timestamp parse(String string) throws ParseException {

    LocalDateTime ldt = LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
    String substring;
    int val;
    int timeZoneSignum=0, timeZoneMinutes=0;

    int index=0;
    for (Token token : tokens) {
      switch (token.type) {
      case TEMPORAL:
        substring = getNextSubstring(string, token, index); // e.g. yy-m -> yy
        val = parseTemporal(substring, token); // e.g. 18->2018, July->07
        ldt = ldt.with(token.temporalField, val);
        index += substring.length();
        break;
      case SEPARATOR:
        index += token.length;
        if (index > string.length()) {
          index = string.length(); // todo ? ALLOW TRAILING SEPARATORS IN PATTERN?
        }
        break;
      case TIMEZONE:
        substring = getNextSubstring(string, token, index); // e.g. yy-m -> yy
        val = Integer.valueOf(substring);
        if (token.temporalUnit == ChronoUnit.HOURS) {
          timeZoneSignum = "-".equals(substring.substring(0,1)) ? -1 : 1 ;
          ldt = ldt.minus(val, token.temporalUnit);
        } else {
          timeZoneMinutes = val;
        }
        index += substring.length();
        break;
      }
    }
    // time zone minutes (process here because sign comes from time zone hour)
    ldt = ldt.minus(timeZoneSignum * timeZoneMinutes, ChronoUnit.MINUTES);

    // deal with potential TimestampLocalTZ time zone ID at end of string
    ZoneId zoneId = ZoneOffset.UTC;
    if (index != string.length()) {
      substring = string.substring(index).trim();
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
  private String getNextSubstring(String s, Token token, int begin) {
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
   * todo NumberFormatException
   * @param substring
   * @param token
   * @return
   */
  private int parseTemporal(String substring, Token token) throws ParseException {
    // exceptions to the rule
    if (token.temporalField == ChronoField.AMPM_OF_DAY) {
      return substring.toLowerCase().startsWith("a") || substring.isEmpty() ? 0 : 1;
    } else if (token.temporalField == ChronoField.YEAR) {
      String currentYearString = String.valueOf(LocalDateTime.now().getYear());
      if (token.string.startsWith("r") && substring.length() == 2) {
        int first2Digits = Integer.valueOf(currentYearString.substring(0, 2));
        int currLast2Digits = Integer.valueOf(currentYearString.substring(2));
        int valLast2Digits = Integer.valueOf(substring);
        if (valLast2Digits < 50 && currLast2Digits >= 50) {
          first2Digits += 1;
        } else if (valLast2Digits >= 50 && currLast2Digits < 50) {
          first2Digits -= 1;
        }
        substring = String.valueOf(first2Digits) + substring;
      } else {
//      fill in prefix digits with current date
        substring = currentYearString.substring(0, 4-substring.length()) + substring;
      }
      //If the specified two-digit year is 00 to 49, then
      //  If the last two digits of the current year are 00 to 49, then the returned year has the same first two digits as the current year.
      //  If the last two digits of the current year are 50 to 99, then the first 2 digits of the returned year are 1 greater than the first 2 digits of the current year.
      //If the specified two-digit year is 50 to 99, then
      //  If the last two digits of the current year are 00 to 49, then the first 2 digits of the returned year are 1 less than the first 2 digits of the current year.
      //  If the last two digits of the current year are 50 to 99, then the returned year has the same first two digits as the current year.
      
    } else if (token.temporalField == ChronoField.NANO_OF_SECOND) {
      int i = Integer.min(token.length, substring.length());
      substring += StringUtils.repeat("0", 9-i);
      
//    } else if () {
      
    }
    // the rule
    try {
      return Integer.valueOf(substring);
    } catch (NumberFormatException e) {
      throw new ParseException("Couldn't parse substring " + substring + " with token " + token +  " to int. Pattern is " + pattern, e);
    }
  }
  
//  private int parseTimeZone(String substring, Token token) {
//    
//  }

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
