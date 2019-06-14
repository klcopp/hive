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
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneOffset;
import java.util.TimeZone;

/**
 * Tests HiveSimpleDateFormatter.
 */
public class TestHiveSimpleDateFormatter {

  private HiveDateTimeFormatter formatter =
      new HiveSimpleDateFormatter("yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone(ZoneOffset.UTC));

  @Test
  public void testFormat() {
    verifyFormat("2019-01-01 01:01:01");
    verifyFormat("2019-01-01 00:00:00");
    verifyFormat("1960-01-01 23:00:00");
  }

  private void verifyFormat(String s) {
    Timestamp ts = Timestamp.valueOf(s);
    Assert.assertEquals(s, formatter.format(ts));
  }

  @Test
  public void testParse() {
    verifyParse("2019-01-01 01:10:10");
    verifyParse("1960-01-01 23:00:00");

  }

  private void verifyParse(String s) {
    Timestamp ts = Timestamp.valueOf(s);
    Assert.assertEquals(ts, formatter.parseTimestamp(s));
  }
}
