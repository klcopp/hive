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
package org.apache.hadoop.hive.common.type;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter;
import org.apache.hadoop.hive.common.format.datetime.HiveDateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampTZUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TimestampTZ.class);

  public static TimestampTZ parse(String s) {
    return parse(s, null);
  }

  public static TimestampTZ parse(String s, ZoneId defaultTimeZone) {
    return DefaultHiveSqlDateTimeFormatter.parseTimestampTZ(s, defaultTimeZone);
  }


  public static TimestampTZ parseOrNull(String s, ZoneId defaultTimeZone) {
    return parseOrNull(s, defaultTimeZone, null);
  }

  public static TimestampTZ parseOrNull(
      String s, ZoneId convertToTimeZone, HiveDateTimeFormatter formatter) {
    if (formatter == null) {
      return parseOrNull(s, convertToTimeZone);
    }

    try {
      return formatter.parseTimestampTZ(s);
    } catch (IllegalArgumentException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Invalid string " + s + " for TIMESTAMP WITH TIME ZONE", e);
      }
      return null;
    }
  }

  // Converts Date to TimestampTZ.
  public static TimestampTZ convert(Date date, ZoneId defaultTimeZone) {
    return parse(date.toString(), defaultTimeZone);
  }

  // Converts Timestamp to TimestampTZ.
  public static TimestampTZ convert(Timestamp ts, ZoneId defaultTimeZone) {
    return parse(ts.toString(), defaultTimeZone);
  }

  public static ZoneId parseTimeZone(String timeZoneStr) {
    if (timeZoneStr == null || timeZoneStr.trim().isEmpty() ||
        timeZoneStr.trim().toLowerCase().equals("local")) {
      // default
      return ZoneId.systemDefault();
    }
    try {
      return ZoneId.of(timeZoneStr);
    } catch (DateTimeException e1) {
      // default
      throw new RuntimeException("Invalid time zone displacement value", e1);
    }
  }

  /**
   * Timestamps are technically time zone agnostic, and this method sort of cheats its logic.
   * Timestamps are supposed to represent nanos since [UTC epoch]. Here,
   * the input timestamp represents nanoseconds since [epoch at fromZone], and
   * we return a Timestamp representing nanoseconds since [epoch at toZone].
   */
  public static Timestamp convertTimestampToZone(Timestamp ts, ZoneId fromZone, ZoneId toZone) {
    // get nanos since [epoch at fromZone]
    Instant instant = convert(ts, fromZone).getZonedDateTime().toInstant();
    // get [local time at toZone]
    LocalDateTime localDateTimeAtToZone = LocalDateTime.ofInstant(instant, toZone);
    // get nanos between [epoch at toZone] and [local time at toZone]
    return Timestamp.ofEpochSecond(localDateTimeAtToZone.toEpochSecond(ZoneOffset.UTC),
        localDateTimeAtToZone.getNano());
  }
}
