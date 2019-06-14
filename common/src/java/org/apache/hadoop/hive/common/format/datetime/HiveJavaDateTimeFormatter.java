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

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Wrapper for DateTimeFormatter in the java.time package.
 */
public class HiveJavaDateTimeFormatter implements HiveDateTimeFormatter {

  private DateTimeFormatter formatter;

  public HiveJavaDateTimeFormatter(DateTimeFormatter formatter) {
    this.formatter = formatter;
  }

  @Override public String format(Timestamp ts) {
    return formatter.format(
        LocalDateTime.ofInstant(
            Instant.ofEpochSecond(ts.toEpochSecond(), ts.getNanos()), ZoneId.of("UTC")));
  }

  @Override public String format(Date date) {
    return format(Timestamp.ofEpochMilli(date.toEpochMilli()));
  }

  @Override public Timestamp parseTimestamp(String string) {
    LocalDateTime ldt = LocalDateTime.parse(string, formatter);
    return Timestamp.ofEpochSecond(ldt.toEpochSecond(ZoneOffset.UTC), ldt.getNano());
  }

  @Override public Date parseDate(String string) {
    return Date.ofEpochMilli(parseTimestamp(string).toEpochMilli());
  }

  @Override public String getPattern() {
    return formatter.toString();
  }
}
