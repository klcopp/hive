/**
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

import org.apache.hadoop.hive.common.type.Timestamp;

import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * Formatter using SQL:2016 datetime patterns.
 */

public class HiveSqlDateTimeFormatter implements HiveDateTimeFormatter {

  private String pattern;

  public HiveSqlDateTimeFormatter() {}

  @Override public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  @Override public String format(Timestamp ts) {
    //TODO replace with actual implementation:
    HiveDateTimeFormatter formatter = new HiveSimpleDateFormatter();
    formatter.setPattern(pattern);
    formatter.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
    return formatter.format(ts);
  }

  @Override public Timestamp parse(String string) throws ParseException {
    //TODO replace with actual implementation:
    HiveDateTimeFormatter formatter = new HiveSimpleDateFormatter();
    formatter.setPattern(pattern);
    formatter.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
    return formatter.parse(string);
  }

  // unused methods
  @Override public void setTimeZone(TimeZone timeZone) {} //frogmethod might be needed for tsLocalTz
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
