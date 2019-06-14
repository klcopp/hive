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

import org.apache.hadoop.hive.common.type.Timestamp;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Wrapper for java.text.SimpleDateFormat.
 */
public class HiveSimpleDateFormatter implements HiveDateTimeFormatter {

  private SimpleDateFormat format = new SimpleDateFormat();
  private String pattern;

  public HiveSimpleDateFormatter(String pattern, TimeZone timeZone) {
    setPattern(pattern);
    format.setTimeZone(timeZone);
  }

  @Override public String format(Timestamp ts) {
    Date date = new Date(ts.toEpochMilli());
    return format.format(date);
  }

  @Override public String format(org.apache.hadoop.hive.common.type.Date d) {
    Date date = new Date(d.toEpochMilli());
    return format.format(date);
  }

  @Override public Timestamp parseTimestamp(String string) {
    try {
      Date date = format.parse(string);
      return Timestamp.ofEpochMilli(date.getTime());
    } catch (java.text.ParseException e) {
      throw new IllegalArgumentException(
          "String " + string + " could not be parsed by java.text.SimpleDateFormat: " + format);
    }
  }

  @Override public org.apache.hadoop.hive.common.type.Date parseDate(String string) {
    return org.apache.hadoop.hive.common.type.Date.ofEpochMilli(
        parseTimestamp(string).toEpochMilli());
  }

  private void setPattern(String pattern) {
    format.applyPattern(pattern);
    this.pattern = pattern;
  }

  @Override public String getPattern() {
    return pattern;
  }
}
