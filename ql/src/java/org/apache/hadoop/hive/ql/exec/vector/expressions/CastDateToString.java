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
import org.apache.hadoop.hive.common.format.datetime.HiveSimpleDateFormatter;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;

import java.sql.Date;
import java.util.TimeZone;

public class CastDateToString extends LongToStringUnaryUDF {
  private static final long serialVersionUID = 1L;
  protected transient Date dt = new Date(0);
  private transient HiveDateTimeFormatter formatter;

  public CastDateToString() {
    super();
    initFormatter();
  }

  public CastDateToString(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
    initFormatter();
  }

  private void initFormatter() {
    formatter = new HiveSimpleDateFormatter();
    formatter.setPattern("yyyy-MM-dd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  // The assign method will be overridden for CHAR and VARCHAR.
  protected void assign(BytesColumnVector outV, int i, byte[] bytes, int length) {
    outV.setVal(i, bytes, 0, length);
  }

  @Override
  protected void func(BytesColumnVector outV, long[] vector, int i) {
    byte[] temp = formatter.format(
        Timestamp.ofEpochMilli(
            org.apache.hadoop.hive.common.type.Date.ofEpochDay((int) vector[i]).toEpochMilli()))
        .getBytes();
    assign(outV, i, temp, temp.length);
  }
}
