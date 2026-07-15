/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.benchmark;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.jackrabbit.util.ISO8601;

public class ISO8601FormatterTest extends AbstractTest<Object> {

    private static final String TYPE = System.getProperty("ISO8601FormatterTest", "jcr-commons-cal");

    private static int COUNT = 1000000;

    private static TimeZone TZ = TimeZone.getTimeZone("UTC");

    private static final DateFormat DTF7 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private static final DateTimeFormatter DTF8 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
            .withZone(ZoneId.of("UTC"));

    @Override
    protected void runTest() throws Exception {
        long chars = 0;
        if ("jcr-commons-cal".equals(TYPE)) {
            for (int i = 0; i < COUNT; i++) {
                Calendar c = Calendar.getInstance(TZ);
                c.setTimeInMillis(((long) i) << 8);
                chars += ISO8601.format(c).length();
            }
// in jackrabbit trunk...
//        } else if ("jcr-commons-long".equals(TYPE)) {
//            for (int i = 0; i < COUNT; i++) {
//                chars += ISO8601.format(((long) i) << 8).length();
//            }
        } else if ("jdk-7-formatter-st".equals(TYPE)) {
            for (int i = 0; i < COUNT; i++) {
                synchronized (DTF7) {
                    chars += DTF7.format(new Date(((long) i) << 8)).length();
                }
            }
        } else if ("jdk-7-formatter-mt".equals(TYPE)) {
            ThreadLocal<DateFormat> formatter = new ThreadLocal<DateFormat>();
            formatter.set(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
            for (int i = 0; i < COUNT; i++) {
                chars += formatter.get().format(new Date(((long) i) << 8)).length();
            }
        } else if ("jdk-8-formatter".equals(TYPE)) {
            for (int i = 0; i < COUNT; i++) {
                chars += DTF8.format(Instant.ofEpochMilli(((long) i) << 8)).length();
            }
        }
        if (chars != 24 * COUNT && chars != 28 * COUNT) {
            System.err.println("formatted strings did not have the expected length: " + chars);
        }
    }
}
