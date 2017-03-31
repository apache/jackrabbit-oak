/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.benchmark.util;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

/**
 * Enumerates some Calendar with math applied for easying tests
 */
public enum Date {
    /**
     * what could be considered the current timestamp
     */
    NOW(Calendar.getInstance()),
    
    /**
     * given {@code NOW} less 2 hours
     */
    LAST_2_HRS(add(NOW.getCalendar(), Calendar.HOUR_OF_DAY, -2)),
    
    /**
     * given {@code NOW} less 24 hours
     */
    LAST_24_HRS(add(NOW.getCalendar(), Calendar.HOUR_OF_DAY, -24)),
    
    /**
     * given {@code NOW} less 1 week
     */
    LAST_7_DAYS(add(NOW.getCalendar(), Calendar.HOUR_OF_DAY, -24*7)),
    
    /**
     * given {@code NOW} less 1 month
     */
    LAST_MONTH(add(NOW.getCalendar(), Calendar.MONTH, -1)),
    
    /**
     * given {@code NOW} less 1 year
     */
    LAST_YEAR(add(NOW.getCalendar(), Calendar.YEAR, -1));

    /**
     * perform math on the provided calendar and return it.
     * 
     * @param cal the calendar to add value to
     * @param field the field to change
     * @param amount the amount to be added
     * @return the changed calendar
     */
    private static Calendar add(Calendar cal, int field, int amount) {
        cal.add(field, amount);
        return cal;
    }

    
    private final Calendar cal;
    
    Date(Calendar cal) {
        this.cal = cal;
    }
    
    public Calendar getCalendar() {
        // duplicating the calendar for allowing safe operations from consumers
        Calendar c = Calendar.getInstance();
        c.setTime(cal.getTime());
        return c;
    }

    public String toISO_8601_2000() {
        return convertToISO_8601_2000(getCalendar());
    }

    private static final List<Date> VALUES = Collections.unmodifiableList(Arrays.asList(values()));
    private static final int SIZE = VALUES.size();
    private static final Random RND = new Random(30);
    
    /**
     * return a random Date
     * 
     * @return the date
     */
    public static Date randomDate() {
        return VALUES.get(RND.nextInt(SIZE));
    }
    
    public static String convertToISO_8601_2000(Calendar cal) {
      SimpleDateFormat format = new SimpleDateFormat(
          "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
      format.setTimeZone(TimeZone.getTimeZone("GMT"));
      return format.format(cal.getTime());
    }
}

