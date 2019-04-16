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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.common.format.datetime.HiveDateTimeFormatter;
import org.apache.hadoop.hive.common.format.datetime.HiveJavaDateTimeFormatter;
import org.apache.hadoop.hive.common.format.datetime.WrongFormatterException;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class CastTimestampToString extends TimestampToStringUnaryUDF {
  private static final long serialVersionUID = 1L;
  private static final DateTimeFormatter PRINT_FORMATTER;

  static {
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
    // Date and time parts
    builder.append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    // Fractional part
    builder.optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd();
    PRINT_FORMATTER = builder.toFormatter();
  }

  HiveDateTimeFormatter format;

  public CastTimestampToString() {
    super();
    initFormatter();
  }

  public CastTimestampToString(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
    initFormatter();
  }

  private void initFormatter() {
    try {
      format = new HiveJavaDateTimeFormatter();
      format.setFormatter(PRINT_FORMATTER);
    } catch (WrongFormatterException e) {
      throw new RuntimeException(e);
    }
  }

  // The assign method will be overridden for CHAR and VARCHAR.
  protected void assign(BytesColumnVector outV, int i, byte[] bytes, int length) {
    outV.setVal(i, bytes, 0, length);
  }

  @Override
  protected void func(BytesColumnVector outV, TimestampColumnVector inV, int i) {
    String formattedLocalDateTime = format.format(
        org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(inV.time[i], inV.nanos[i]));

    byte[] temp = formattedLocalDateTime.getBytes();
    assign(outV, i, temp, temp.length);
  }

  public static String getTimestampString(java.sql.Timestamp ts) {
    return
        LocalDateTime.ofInstant(Instant.ofEpochMilli(ts.getTime()), ZoneOffset.UTC)
        .withNano(ts.getNanos())
        .format(PRINT_FORMATTER);
  }

  public static String getTimestampString(java.sql.Timestamp ts, HiveDateTimeFormatter formatter) {
    if (formatter == null) {
      return getTimestampString(ts);
    }
    return formatter.format(Timestamp.ofEpochMilli(ts.getTime(), ts.getNanos()));
  }
}
