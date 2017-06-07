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
package org.apache.jackrabbit.oak.plugins.memory;

import static java.util.Calendar.HOUR_OF_DAY;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyValues.newDate;
import static org.apache.jackrabbit.util.ISO8601.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Calendar;
import java.util.TimeZone;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.junit.Test;

public class PropertyStateValueTest {
    /*
     * GMT
     */
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");
    
    /*
     * GMT +1
     */
    private static final TimeZone CET = TimeZone.getTimeZone("CET");
    
    /*
     * GMT -8
     */
    private static final TimeZone PST = TimeZone.getTimeZone("PST");

    @Test
    public void compareDates() {
        Calendar d1, d2;
        PropertyValue v1, v2;

        // same time zones.
        d1 = newCal(null);
        d2 = newCal(d1);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertEquals("v1 and v2 should be equals", 0, v1.compareTo(v2));
        assertEquals("v1 and v2 should be equals", 0, v2.compareTo(v1));

        d1 = newCal(null);
        d2 = newCal(d1, GMT, 1);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertTrue("v1 < v2", v1.compareTo(v2) < 0);
        assertTrue("v2 > v1", v2.compareTo(v1) > 0);

        d1 = newCal(null);
        d2 = newCal(d1, GMT, -1);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertTrue("v1 > v2", v1.compareTo(v2) > 0);
        assertTrue("v2 < v1", v2.compareTo(v1) < 0);
        
        // same time zone. Non Zulu.
        d1 = newCal(null, PST, 0);
        d2 = newCal(d1, PST, 0);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertEquals("v1 and v2 should be equals", 0, v1.compareTo(v2));
        assertEquals("v1 and v2 should be equals", 0, v2.compareTo(v1));

        d1 = newCal(null, PST, 0);
        d2 = newCal(d1, PST, 1);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertTrue("v1 < v2", v1.compareTo(v2) < 0);
        assertTrue("v2 > v1", v2.compareTo(v1) > 0);

        d1 = newCal(null, PST, 0);
        d2 = newCal(d1, PST, -1);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertTrue("v1 > v2", v1.compareTo(v2) > 0);
        assertTrue("v2 < v1", v2.compareTo(v1) < 0);

        // ahead time zone
        d1 = newCal(null);
        d2 = newCal(d1, CET, 0);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertEquals("v1 and v2 should be equals", 0, v1.compareTo(v2));
        assertEquals("v1 and v2 should be equals", 0, v2.compareTo(v1));

        d1 = newCal(null);
        d2 = newCal(d1, CET, 1);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertTrue("v1 < v2", v1.compareTo(v2) < 0);
        assertTrue("v2 > v1", v2.compareTo(v1) > 0);

        d1 = newCal(null);
        d2 = newCal(d1, CET, -1);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertTrue("v1 > v2", v1.compareTo(v2) > 0);
        assertTrue("v2 < v1", v2.compareTo(v1) < 0);
        
        // behind time zone
        d1 = newCal(null);
        d2 = newCal(d1, PST, 0);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertEquals("v1 and v2 should be equals", 0, v1.compareTo(v2));
        assertEquals("v1 and v2 should be equals", 0, v2.compareTo(v1));

        d1 = newCal(null);
        d2 = newCal(d1, PST, 1);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertTrue("v1 < v2", v1.compareTo(v2) < 0);
        assertTrue("v2 > v1", v2.compareTo(v1) > 0);

        d1 = newCal(null);
        d2 = newCal(d1, PST, -1);
        v1 = newDate(format(d1)); 
        v2 = newDate(format(d2));
        assertTrue("v1 > v2", v1.compareTo(v2) > 0);
        assertTrue("v2 < v1", v2.compareTo(v1) < 0);
    }
    
    /**
     * same as {@link #newCal(Calendar, TimeZone, int)} passing {@code null} as {@code TimeZone} and
     * {@code 0} as {@code int}
     * 
     * @param start
     * @return
     */
    private static Calendar newCal(@Nullable final Calendar start) {
        return newCal(start, null, 0);
    }
    
    /**
     * return a new Calendar instance with the same time as the one provided as input.
     * 
     * @param start the calendar we want the new one have the same time of. If null it will be a new
     *            calendar instance
     * @param the desired time zone. If null, GMT will be used.
     * @param hoursOffset how many hours should we move the clock.
     * @return
     */
    private static Calendar newCal(@Nullable final Calendar start,
                                   @Nullable final TimeZone tz,
                                   final int hoursOffset) {
        Calendar c = Calendar.getInstance();
        c.setTimeZone(tz == null ? GMT : tz);
        if (start == null) {
            return c;
        } else {
            c.setTime(start.getTime());
            c.add(HOUR_OF_DAY, hoursOffset);
            return c;
        }
    }
}
