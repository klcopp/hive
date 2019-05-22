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

import org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter;
import org.apache.hadoop.hive.common.format.datetime.HiveDateTimeFormatter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * This is the internal type for Timestamp.
 * The full qualified input format of Timestamp is
 * "yyyy-MM-dd HH:mm:ss[.SSS...]", where the time part is optional.
 * If time part is absent, a default '00:00:00.0' will be used.
 */
public class Timestamp implements Comparable<Timestamp> {
  
  private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0);

  private LocalDateTime localDateTime;

  /* Private constructor */
  private Timestamp(LocalDateTime localDateTime) {
    this.localDateTime = localDateTime != null ? localDateTime : EPOCH;
  }

  public Timestamp() {
    this(EPOCH);
  }

  public Timestamp(Timestamp t) {
    this(t.localDateTime);
  }

  public void set(Timestamp t) {
    this.localDateTime = t != null ? t.localDateTime : EPOCH;
  }

  public String format(DateTimeFormatter formatter) {
    return localDateTime.format(formatter);
  }

  @Override
  public String toString() {
    return DefaultHiveSqlDateTimeFormatter.format(this);
  }

  public String toStringFormatted(HiveDateTimeFormatter formatter) {
    if (formatter == null) {
      return toString();
    }
    try {
      return formatter.format(this);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  public int hashCode() {
    return localDateTime.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Timestamp) {
      return compareTo((Timestamp) other) == 0;
    }
    return false;
  }

  @Override
  public int compareTo(Timestamp o) {
    return localDateTime.compareTo(o.localDateTime);
  }

  public long toEpochSecond() {
    return localDateTime.toEpochSecond(ZoneOffset.UTC);
  }

  public void setTimeInSeconds(long epochSecond) {
    setTimeInSeconds(epochSecond, 0);
  }

  public void setTimeInSeconds(long epochSecond, int nanos) {
    localDateTime = LocalDateTime.ofEpochSecond(
        epochSecond, nanos, ZoneOffset.UTC);
  }

  public long toEpochMilli() {
    return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public void setTimeInMillis(long epochMilli) {
    localDateTime = LocalDateTime.ofInstant(
        Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC);
  }

  public void setTimeInMillis(long epochMilli, int nanos) {
    localDateTime = LocalDateTime
        .ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC)
        .withNano(nanos);
  }

  public int getNanos() {
    return localDateTime.getNano();
  }

  //todo frogmethod throws illegalargumetnexception "Cannot create timestamp, parsing error"
  public static Timestamp valueOf(String s) {
    return DefaultHiveSqlDateTimeFormatter.parseTimestamp(s.trim());
  }

  public static Timestamp valueOf(String s, HiveDateTimeFormatter formatter) {
    if (formatter == null) {
      return valueOf(s);
    }
    return formatter.parseTimestamp(s);
  }

  public static Timestamp ofEpochSecond(long epochSecond) {
    return ofEpochSecond(epochSecond, 0);
  }

  public static Timestamp ofEpochSecond(long epochSecond, int nanos) {
    return new Timestamp(
        LocalDateTime.ofEpochSecond(epochSecond, nanos, ZoneOffset.UTC));
  }

  public static Timestamp ofEpochMilli(long epochMilli) {
    return new Timestamp(LocalDateTime
        .ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC));
  }

  public static Timestamp ofEpochMilli(long epochMilli, int nanos) {
    return new Timestamp(LocalDateTime
        .ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC)
        .withNano(nanos));
  }

  public void setNanos(int nanos) {
    localDateTime = localDateTime.withNano(nanos);
  }

  public int getYear() {
    return localDateTime.getYear();
  }

  public int getMonth() {
    return localDateTime.getMonthValue();
  }

  public int getDay() {
    return localDateTime.getDayOfMonth();
  }

  public int getHours() {
    return localDateTime.getHour();
  }

  public int getMinutes() {
    return localDateTime.getMinute();
  }

  public int getSeconds() {
    return localDateTime.getSecond();
  }

  public int getDayOfWeek() {
    return localDateTime.getDayOfWeek().plus(1).getValue();
  }

  /**
   * Return a copy of this object.
   */
  public Object clone() {
    // LocalDateTime is immutable.
    return new Timestamp(this.localDateTime);
  }

  public java.sql.Timestamp toSqlTimestamp() {
    java.sql.Timestamp ts = new java.sql.Timestamp(toEpochMilli());
    ts.setNanos(getNanos());
    return ts;
  }

}
