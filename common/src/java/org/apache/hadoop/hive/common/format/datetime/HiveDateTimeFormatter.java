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

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * Interface used for formatting and parsing timestamps. Initially created so that user is able to
 * optionally format datetime objects into strings and parse strings into datetime objects with
 * SQL:2016 semantics.
 */
public interface HiveDateTimeFormatter {
  /**
   * Format the given timestamp into a string.
   * 
   * @throws IllegalArgumentException if timestamp cannot be formatted.
   */
  String format(Timestamp ts);

  /**
   * Format the given date into a string.
   * 
   * @throws IllegalArgumentException if date cannot be formatted.
   */
  String format(Date date);

  /**
   * Format the given timestamp with local time zone into a string.
   * 
   * @throws IllegalArgumentException if timestampTZ cannot be formatted.
   */
  String format(TimestampTZ timestampTZ);

  /**
   * Parse the given string into a timestamp.
   *
   * @throws IllegalArgumentException if string cannot be parsed.
   */
  Timestamp parseTimestamp(String string);

  /**
   * Parse the given string into a timestamp.
   *
   * @throws IllegalArgumentException if string cannot be parsed.
   */
  Date parseDate(String string);

  /**
   * Parse the given string into a timestamp.
   *
   * @throws IllegalArgumentException if string cannot be parsed.
   */
  TimestampTZ parseTimestampTZ(String string);

  /**
   * Set the format pattern to be used for formatting timestamps or parsing strings.
   * This method parses the pattern into tokens, so it comes with some performance overhead.
   */
  void setPattern(String pattern, boolean forParsing) throws IllegalArgumentException;

  /**
   * Get the format pattern to be used for formatting timestamps or parsing strings.
   */
  String getPattern();

  /**
   * Set the time zone of the formatter.
   */
  void setTimeZone(TimeZone timeZone);

}
