package org.apache.jackrabbit.oak.commons;
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

import static org.junit.Assert.assertEquals;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TimeDurationFormatterTest {

    private TimeDurationFormatter df = TimeDurationFormatter.forLogging();
    private TimeDurationFormatter dfu = new TimeDurationFormatter(Locale.GERMAN, 1, true);

    @Test
    public void defaultFormatter() {
        assertEquals("2.000 d", df.format(2, TimeUnit.DAYS));
        assertEquals("24.00 h", df.format(1, TimeUnit.DAYS));
        assertEquals("10.50 d", df.format(24 * 10 + 12, TimeUnit.HOURS));
        assertEquals("2.500 h", df.format(150, TimeUnit.MINUTES));
        assertEquals("10.00 min", df.format(10, TimeUnit.MINUTES));
        assertEquals("1234 us", df.format(1234, TimeUnit.MICROSECONDS));
        assertEquals("1234 ns", df.format(1234, TimeUnit.NANOSECONDS));
    }

    @Test
    public void uniFormatterWithOneThreshold() {
        assertEquals("2,000 d", dfu.format(2, TimeUnit.DAYS));
        assertEquals("1,000 d", dfu.format(1, TimeUnit.DAYS));
        assertEquals("10,50 d", dfu.format(24 * 10 + 12, TimeUnit.HOURS));
        assertEquals("2,500 h", dfu.format(150, TimeUnit.MINUTES));
        assertEquals("10,00 min", dfu.format(10, TimeUnit.MINUTES));
        assertEquals("1,234 ms", dfu.format(1234, TimeUnit.MICROSECONDS));
        assertEquals("1,234 \u00b5s", dfu.format(1234, TimeUnit.NANOSECONDS));
    }
}
