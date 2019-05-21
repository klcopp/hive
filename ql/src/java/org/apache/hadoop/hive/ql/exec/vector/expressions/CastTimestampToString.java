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

import org.apache.hadoop.hive.common.format.datetime.DefaultHiveSqlDateTimeFormatter;
import org.apache.hadoop.hive.common.format.datetime.HiveDateTimeFormatter;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;

public class CastTimestampToString extends TimestampToStringUnaryUDF {
  private static final long serialVersionUID = 1L;

  public CastTimestampToString() {
    super();
  }

  public CastTimestampToString(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  // The assign method will be overridden for CHAR and VARCHAR.
  protected void assign(BytesColumnVector outV, int i, byte[] bytes, int length) {
    outV.setVal(i, bytes, 0, length);
  }

  private void assignNull(BytesColumnVector outV, int i) {
    outV.isNull[i] = true;
    outV.noNulls = false;
  }

  @Override
  protected void func(BytesColumnVector outV, TimestampColumnVector inV, int i) {
    func(outV, inV, i, null);
  }

  protected void func(BytesColumnVector outV, TimestampColumnVector inV, int i, HiveDateTimeFormatter formatter) {
    try {
      Timestamp timestamp = Timestamp.ofEpochMilli(inV.time[i], inV.nanos[i]);
      String output = (formatter != null) ? formatter.format(timestamp) :
          DefaultHiveSqlDateTimeFormatter.format(timestamp);
      byte[] temp = output.getBytes();
      assign(outV, i, temp, temp.length);
    } catch (Exception e) {
      assignNull(outV, i);
    }
  }
  public static String getTimestampString(java.sql.Timestamp ts) {
    return DefaultHiveSqlDateTimeFormatter.format(
        Timestamp.ofEpochMilli(ts.getTime(), ts.getNanos()));
  }
}
