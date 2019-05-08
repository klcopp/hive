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

import java.util.TimeZone;

import org.apache.hadoop.hive.common.format.datetime.HiveDateTimeFormatter;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFFromUnixTime.
 *
 */
@Description(name = "from_unixtime",
    value = "_FUNC_(unix_time, format) - returns unix_time in the specified format",
    extended = "format is a String which specifies the format for output. If session-level "
        + "setting hive.use.sql.datetime.formats is true, "
        + "output_date_format will be interpreted as SQL:2016 datetime format. Otherwise it will "
        + "be interpreted as java.text.SimpleDateFormat.\n"
        + "Example:\n"
    + "  > SELECT _FUNC_(0, 'yyyy-MM-dd HH:mm:ss') FROM src LIMIT 1;\n"
    + "  '1970-01-01 00:00:00'")
public class UDFFromUnixTime extends UDF {
  private HiveDateTimeFormatter formatter;
  private boolean useSqlFormat = true;
  private boolean lastUsedSqlFormats = true;

  private Text result = new Text();
  private Text lastFormat = new Text();

  public UDFFromUnixTime() {
  }

  private Text defaultFormat = new Text("yyyy-MM-dd HH:mm:ss");

  public Text evaluate(IntWritable unixtime) {
    return evaluate(unixtime, defaultFormat);
  }

  /**
   * Convert UnixTime to a string format.
   * 
   * @param unixtime
   *          The number of seconds from 1970-01-01 00:00:00
   * @param format
   *          See http://java.sun.com/j2se/1.4.2/docs/api/java/text/SimpleDateFormat.html,
   *          or set hive.use.sql.datetime.formats=true to use SQL:2016 formats.
   * @return a String in the format specified.
   */
  public Text evaluate(LongWritable unixtime, Text format) {
    if (unixtime == null || format == null) {
      return null;
    }

    return eval(unixtime.get(), format);
  }
  
  /**
   * Convert UnixTime to a string format.
   * 
   * @param unixtime
   *          The number of seconds from 1970-01-01 00:00:00
   * @return a String in default format specified.
   */
  public Text evaluate(LongWritable unixtime) {
    if (unixtime == null) {
      return null;
    }

    return eval(unixtime.get(), defaultFormat);
  }

  /**
   * Convert UnixTime to a string format.
   * 
   * @param unixtime
   *          The number of seconds from 1970-01-01 00:00:00
   * @param format
   *          See http://java.sun.com/j2se/1.4.2/docs/api/java/text/SimpleDateFormat.html,
   *          or set hive.use.sql.datetime.formats=true to use SQL:2016 formats.
   * @return a String in the format specified.
   */
  public Text evaluate(IntWritable unixtime, Text format) {
    if (unixtime == null || format == null) {
      return null;
    }

    return eval(unixtime.get(), format);
  }

  /**
   * Internal evaluation function given the seconds from 1970-01-01 00:00:00 and
   * the output text format.
   * 
   * @param unixtime
   *          seconds of type long from 1970-01-01 00:00:00
   * @param format
   *          display format.
   *          See http://java.sun.com/j2se/1.4.2/docs/api/java/text/SimpleDateFormat.html,
   *          or set hive.use.sql.datetime.formats=true to use SQL:2016 formats.
   * @return elapsed time in the given format.
   */
  private Text eval(long unixtime, Text format) {
    initFormatter();

    if (!format.equals(lastFormat)) {
      formatter.setPattern(format.toString(), false);
      lastFormat.set(format);
    }

    // convert seconds to milliseconds
    Timestamp ts = Timestamp.ofEpochMilli(unixtime * 1000L);
    result.set(formatter.format(ts));
    return result;
  }

  private void initFormatter() {
    useSqlFormat = GenericUDF.useSqlFormat();
    if (formatter == null || useSqlFormat != lastUsedSqlFormats) {
      formatter = GenericUDF.getHiveDateTimeFormatter(useSqlFormat);
      formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
      lastUsedSqlFormats = useSqlFormat;
    }
  }
}
