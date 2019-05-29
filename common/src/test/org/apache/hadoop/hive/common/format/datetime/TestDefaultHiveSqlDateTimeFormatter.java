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

import junit.framework.TestCase;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;

import static org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter.getNumberOfTokenGroups;
import static org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter.parseDate;
import static org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter.parseTimestamp;

/**
 * Tests methods in class DefaultHiveSqlDateTimeFormatter.
 */
public class TestDefaultHiveSqlDateTimeFormatter extends TestCase {
  private static final Timestamp TS_2019_01_01__02_03_04 = Timestamp.ofEpochMilli(1546308184000L);
  private static final Date D_2019_01_01 = Date.ofEpochMilli(1546300800000L);


  private Timestamp timestamp(Timestamp input, int nanos) {
    Timestamp output = (Timestamp) input.clone();
    output.setNanos(nanos);
    return output;
  }

  public void testFormatTimestamp() {
    String s1 = "2019-01-01 02:03:04";
    String s2 = "2019-01-01 02:03:04.44444";
    String s3 = "2019-01-01 02:03:04.444444444";
    assertEquals(s1, DefaultHiveSqlDateTimeFormatter.format(TS_2019_01_01__02_03_04));
    assertEquals(s2, DefaultHiveSqlDateTimeFormatter
        .format(timestamp(TS_2019_01_01__02_03_04, 444440000)));
    assertEquals(s3, DefaultHiveSqlDateTimeFormatter
        .format(timestamp(TS_2019_01_01__02_03_04, 444444444)));
  }

  public void testFormatDate() {
    String s1 = "2019-01-01";
    assertEquals(s1, DefaultHiveSqlDateTimeFormatter.format(D_2019_01_01));
  }

  public void testParseTimestamp() {
    String s1 = "2019-01-01 02:03:04";
    String s2 = "2019-01-01 02:03:04.000";
    String s3 = "2019-01-01T02:03:04Z";
    String s4 = "2019-01-01 02:03:04.44444";
    String s5 = "2019-01-01T02:03:04.44444Z";
    String s6 = "2019.01.01T02....03:04..44444Z";
    String s7 = "2019-01-01T02:03:04";
    String s8 = "2019-01-01T02:03:04.444440";

    assertEquals(TS_2019_01_01__02_03_04, parseTimestamp(s1));
    assertEquals(TS_2019_01_01__02_03_04, parseTimestamp(s2));
    assertEquals(TS_2019_01_01__02_03_04, parseTimestamp(s3));
    assertEquals(timestamp(TS_2019_01_01__02_03_04, 444440000), parseTimestamp(s4));
    assertEquals(timestamp(TS_2019_01_01__02_03_04, 444440000), parseTimestamp(s5));
    assertEquals(timestamp(TS_2019_01_01__02_03_04, 444440000), parseTimestamp(s6));
    assertEquals(TS_2019_01_01__02_03_04, parseTimestamp(s7));
    assertEquals(timestamp(TS_2019_01_01__02_03_04, 444440000), parseTimestamp(s8));
  }

  public void testParseDate() {
    String s1 = "2019/01///01";
    String s2 = "19/01///01";
    String s3 = "2019-01-01T02:03:04Z";
    String s4 = "2019-01-01 02:03:04.44444";
    String s5 = "2019-01-01T02:03:04.44444Z";
    String s6 = "2019.01.01T02....03:04..44444";
    assertEquals(D_2019_01_01, parseDate(s1));
    assertEquals(D_2019_01_01, parseDate(s2));
    assertEquals(D_2019_01_01, parseDate(s3));
    assertEquals(D_2019_01_01, parseDate(s4));
    assertEquals(D_2019_01_01, parseDate(s5));
    assertEquals(D_2019_01_01, parseDate(s6));
  }

  public void testGetNumberOfTokenGroups() {
    assertEquals(4, getNumberOfTokenGroups("2018..39..7T urkey"));
    assertEquals(8, getNumberOfTokenGroups("2019-01-01T02:03:04Z"));
    assertEquals(3, getNumberOfTokenGroups("2019-01-01GMT"));
    assertEquals(4, getNumberOfTokenGroups("2019-01-01Turkey"));
  }
}
