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

import org.apache.hadoop.hive.common.type.Timestamp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Test class for HiveJavaDateTimeFormatter.
 */
public class TestHiveJavaDateTimeFormatter {

  private static final DateTimeFormatter DATE_TIME_FORMATTER;
  static {
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
    builder.append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    builder.optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd();
    DATE_TIME_FORMATTER = builder.toFormatter();
  }
  private HiveDateTimeFormatter formatter = new HiveJavaDateTimeFormatter();

  @Before
  public void setUp() throws WrongFormatterException {
    formatter.setFormatter(DATE_TIME_FORMATTER);
  }

  @Test
  public void testFormat() throws FormatException {
    Timestamp ts = Timestamp.valueOf("2019-01-01 00:00:00.99999");
    Assert.assertEquals("2019-01-01 00:00:00.99999", formatter.format(ts));
  }

  @Test
  public void testParse() throws ParseException {
    String s = "2019-01-01 00:00:00.99999";
    Assert.assertEquals(Timestamp.valueOf("2019-01-01 00:00:00.99999"), formatter.parse(s));
  }

}
