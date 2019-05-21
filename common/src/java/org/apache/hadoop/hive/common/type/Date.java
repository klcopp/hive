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
import org.apache.hadoop.hive.common.format.datetime.FormatException;
import org.apache.hadoop.hive.common.format.datetime.HiveDateTimeFormatter;
import org.apache.hadoop.hive.common.format.datetime.ParseException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * This is the internal type for Date.
 * The full qualified input format of Date is "yyyy-MM-dd".
 */
public class Date implements Comparable<Date> {

  private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);

  private LocalDate localDate;

  private Date(LocalDate localDate) {
    this.localDate = localDate != null ? localDate : EPOCH;
  }

  public Date() {
    this(EPOCH);
  }

  public Date(Date d) {
    this(d.localDate);
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
    } catch (FormatException e) {
      return null;
    }
  }

  public int hashCode() {
    return localDate.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Date) {
      return compareTo((Date) other) == 0;
    }
    return false;
  }

  @Override
  public int compareTo(Date o) {
    return localDate.compareTo(o.localDate);
  }

  public int toEpochDay() {
    return (int) localDate.toEpochDay();
  }

  public long toEpochSecond() {
    return localDate.atStartOfDay().toEpochSecond(ZoneOffset.UTC);
  }

  public long toEpochMilli() {
    return localDate.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public void setYear(int year) {
    localDate = localDate.withYear(year);
  }

  public void setMonth(int month) {
    localDate = localDate.withMonth(month);
  }

  public void setDayOfMonth(int dayOfMonth) {
    localDate = localDate.withDayOfMonth(dayOfMonth);
  }

  public void setTimeInDays(int epochDay) {
    localDate = LocalDate.ofEpochDay(epochDay);
  }

  public void setTimeInMillis(long epochMilli) {
    localDate = LocalDateTime.ofInstant(
        Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC).toLocalDate();
  }

  public static Date valueOf(String s) {
    return DefaultHiveSqlDateTimeFormatter.parseDate(s.trim());
  }

  public static Date valueOf(String s, HiveDateTimeFormatter formatter) throws ParseException {
    if (formatter == null) {
      return valueOf(s);
    }
    s = s.trim();
    return formatter.parseDate(s);
  }

  public static Date ofEpochDay(int epochDay) {
    return new Date(LocalDate.ofEpochDay(epochDay));
  }

  public static Date ofEpochMilli(long epochMilli) {
    return new Date(LocalDateTime.ofInstant(
        Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC).toLocalDate());
  }

  public static Date of(int year, int month, int dayOfMonth) {
    return new Date(LocalDate.of(year, month, dayOfMonth));
  }

  public int getYear() {
    return localDate.getYear();
  }

  public int getMonth() {
    return localDate.getMonthValue();
  }

  public int getDay() {
    return localDate.getDayOfMonth();
  }

  public int lengthOfMonth() {
    return localDate.lengthOfMonth();
  }

  public int getDayOfWeek() {
    return localDate.getDayOfWeek().plus(1).getValue();
  }

  /**
   * Return a copy of this object.
   */
  public Object clone() {
    // LocalDateTime is immutable.
    return new Date(this.localDate);
  }

}
