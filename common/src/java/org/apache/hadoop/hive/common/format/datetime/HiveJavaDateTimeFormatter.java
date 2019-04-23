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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * Wrapper for DateTimeFormatter in the java.time package.
 */
public class HiveJavaDateTimeFormatter implements HiveDateTimeFormatter {

  private DateTimeFormatter formatter;

  @Override public void setFormatter(DateTimeFormatter dateTimeFormatter) {
    this.formatter = dateTimeFormatter;
  }

  @Override public String format(Timestamp ts) {
    return formatter.format(
        LocalDateTime.ofInstant(
            Instant.ofEpochSecond(ts.toEpochSecond(), ts.getNanos()), ZoneId.of("UTC")));
  }

  @Override public Timestamp parse(String string) {
    return Timestamp.valueOf(string);
  }

  // unused methods
  @Override public void setPattern(String pattern) {}
  @Override public void setTimeZone(TimeZone timeZone) {}
  @Override public void setFormatter(SimpleDateFormat simpleDateFormat)
      throws WrongFormatterException {
    throw new WrongFormatterException("HiveJavaDateTimeFormatter formatter wraps an object of type"
        + "java.time.format.DateTimeFormatter, formatter cannot be of type "
        + "java.text.SimpleDateFormat");
  }
}
