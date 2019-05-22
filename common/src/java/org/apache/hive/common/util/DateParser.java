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
package org.apache.hive.common.util;

import org.apache.hadoop.hive.common.format.datetime.HiveDateTimeFormatter;
import org.apache.hadoop.hive.common.type.Date;

/**
 * Date parser class for Hive.
 */
public class DateParser {

  public DateParser() {
 }

  public Date parseDate(String strValue) {
    Date result = new Date();
    if (parseDate(strValue, result)) {
      return result;
    }
    return null;
  }

  public boolean parseDate(String strValue, Date result) {
    return parseDate(strValue, result, null);
  }

  public boolean parseDate(String strValue, Date result, HiveDateTimeFormatter formatter) {
    Date parsedVal;
    try {
      parsedVal = Date.valueOf(strValue, formatter);
    } catch (IllegalArgumentException e) {
      parsedVal = null;
    }
    if (parsedVal == null) {
      return false;
    }
    result.setTimeInMillis(parsedVal.toEpochMilli());
    return true;
  }
}
