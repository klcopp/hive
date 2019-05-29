/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.common.format.datetime;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.util.Map;

/**
 * Parse/format datetime objects according to some select default SQL:2016 formats.
 */
public class DefaultHiveSqlDateTimeFormatter {

  private static HiveSqlDateTimeFormatter formatterDate = new HiveSqlDateTimeFormatter();
  private static HiveSqlDateTimeFormatter formatterNoNanos = new HiveSqlDateTimeFormatter();
  private static HiveSqlDateTimeFormatter formatterWithNanos = new HiveSqlDateTimeFormatter();

  private static HiveSqlDateTimeFormatter formatterIsoNoNanos = new HiveSqlDateTimeFormatter();
  private static HiveSqlDateTimeFormatter formatterIsoWithNanos = new HiveSqlDateTimeFormatter();

  private static HiveSqlDateTimeFormatter formatterIsoNoNanosNoZ = new HiveSqlDateTimeFormatter();
  private static HiveSqlDateTimeFormatter formatterIsoWithNanosNoZ = new HiveSqlDateTimeFormatter();

  static {
    //forParsing is false because there's no need to verify pattern
    formatterDate.setPattern("yyyy-mm-dd", false);
    formatterNoNanos.setPattern("yyyy-mm-dd hh24:mi:ss", false);
    formatterWithNanos.setPattern("yyyy-mm-dd hh24:mi:ss.ff", false);

    formatterIsoNoNanos.setPattern("yyyy-mm-ddThh24:mi:ssZ", false);
    formatterIsoWithNanos.setPattern("yyyy-mm-ddThh24:mi:ss.ffZ", false);

    formatterIsoNoNanosNoZ.setPattern("yyyy-mm-ddThh24:mi:ss", false);
    formatterIsoWithNanosNoZ.setPattern("yyyy-mm-ddThh24:mi:ss.ff", false);
  }

  private static final Map<Integer, HiveSqlDateTimeFormatter> TOKEN_COUNT_FORMATTER_MAP =
      ImmutableMap.<Integer, HiveSqlDateTimeFormatter>builder()
          .put(3, formatterDate).put(6, formatterNoNanos).put(7, formatterWithNanos).build();

  private static final Map<Integer, HiveSqlDateTimeFormatter> TOKEN_COUNT_ISO_FORMATTER_MAP =
      ImmutableMap.<Integer, HiveSqlDateTimeFormatter>builder()
          .put(8, formatterIsoNoNanos).put(9, formatterIsoWithNanos).build();

  private static final Map<Integer, HiveSqlDateTimeFormatter> TOKEN_COUNT_ISO_FORMATTER_MAP_NO_Z =
      ImmutableMap.<Integer, HiveSqlDateTimeFormatter>builder()
          .put(7, formatterIsoNoNanosNoZ).put(8, formatterIsoWithNanosNoZ).build();

  private DefaultHiveSqlDateTimeFormatter() {}

  public static String format(Timestamp ts) {
    return (ts.getNanos() == 0) ? formatterNoNanos.format(ts) : formatterWithNanos.format(ts);
  }

  public static String format(Date date) {
    return formatterDate.format(date);
  }

  public static Timestamp parseTimestamp(String input) {
    input = input.trim();
    HiveSqlDateTimeFormatter formatter = getFormatter(input);
    return formatter.parseTimestamp(input);
  }

  public static Date parseDate(String input) {
    HiveSqlDateTimeFormatter formatter = getFormatter(input);
    return formatter.parseDate(input.trim());
  }

  private static HiveSqlDateTimeFormatter getFormatter(String input) {
    Map<Integer, HiveSqlDateTimeFormatter> map;
    if (input.toLowerCase().contains("t")) {
      if (input.toLowerCase().contains("z")) {
        map = TOKEN_COUNT_ISO_FORMATTER_MAP;
      } else {
        map = TOKEN_COUNT_ISO_FORMATTER_MAP_NO_Z;
      }
    } else {
      map = TOKEN_COUNT_FORMATTER_MAP;
    }

    int numberOfTokenGroups = getNumberOfTokenGroups(input);
    if (!map.containsKey(numberOfTokenGroups)) {
      throw new IllegalArgumentException("No available default parser for input: " + input);
    }
    return map.get(numberOfTokenGroups);
  }

  // count number of non-separator tokens
  static int getNumberOfTokenGroups(String input) {
    int count = 0;
    boolean lastCharWasSep = true, isIsoDelimiter;

    for (char c : input.toCharArray()) {
      String s = String.valueOf(c);
      isIsoDelimiter = HiveSqlDateTimeFormatter.VALID_ISO_8601_DELIMITERS.contains(s.toLowerCase());
      if (!HiveSqlDateTimeFormatter.VALID_SEPARATORS.contains(s)) {
        if (!isIsoDelimiter && !Character.isDigit(c)) { // it's probably part of a time zone. Halt.
          break;
        }
        if (lastCharWasSep || isIsoDelimiter) {
          count++;
        }
        // ISO delimiters are... delimiters
        lastCharWasSep = isIsoDelimiter;
      } else {
        lastCharWasSep = true;
      }
    }
    return count;
  }
}
