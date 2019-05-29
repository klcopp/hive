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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests UDFFromUnixTime.
 */
public class TestUDFFromUnixTime {

  @Test
  public void testFromUnixTime() {
    UDFFromUnixTime udf = new UDFFromUnixTime();

    //int, no format
    verifyInt(0, "1970-01-01 00:00:00", null, udf);
    verifyInt(1296705906, "2011-02-03 04:05:06", null, udf);
    verifyInt(1514818800, "2018-01-01 15:00:00", null, udf);

    //long, no format
    verifyLong(0L, "1970-01-01 00:00:00", null, udf);
    verifyLong(1296705906L, "2011-02-03 04:05:06", null, udf);
    verifyLong(1514818800L, "2018-01-01 15:00:00", null, udf);
    // proleptic Gregorian input: -30767590800L
    verifyLong(-30767158800L, "0995-01-05 15:00:00", null, udf);
    // proleptic Gregorian input: -62009366400
    verifyLong(-62009539200L, "0005-01-01 00:00:00", null, udf);
    verifyLong(253402300799L, "9999-12-31 23:59:59", null, udf);

    //int with format
    String format = "HH:mm:ss";
    verifyInt(0, "00:00:00", format, udf);
    verifyInt(1296705906, "04:05:06", format, udf);
    verifyInt(1514818800, "15:00:00", format, udf);

    //long with format
    verifyLong(0L, "00:00:00", format, udf);
    verifyLong(1296705906L, "04:05:06", format, udf);
    verifyLong(1514818800L, "15:00:00", format, udf);
    // proleptic Gregorian input: -30767590800L
    verifyLong(-30767158800L, "15:00:00", format, udf);
    // proleptic Gregorian input: -62009366400
    verifyLong(-62009539200L, "00:00:00", format, udf);
    verifyLong(253402300799L, "23:59:59", format, udf);

  }

  private void verifyInt(int value, String expected, String format, UDFFromUnixTime udf) {
    IntWritable input = new IntWritable(value);
    Text res;
    if (format == null) {
      res = udf.evaluate(input);
    } else {
      res = udf.evaluate(input, new Text(format));
    }
    Assert.assertEquals(expected, res.toString());
  }

  private void verifyLong(long value, String expected, String format, UDFFromUnixTime udf) {
    LongWritable input = new LongWritable(value);
    Text res;
    if (format == null) {
      res = udf.evaluate(input);
    } else {
      res = udf.evaluate(input, new Text(format));
    }
    Assert.assertEquals(expected, res.toString());
  }
}
