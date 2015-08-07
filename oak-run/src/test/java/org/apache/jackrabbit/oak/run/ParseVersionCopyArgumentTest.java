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

package org.apache.jackrabbit.oak.run;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.junit.Assert;
import org.junit.Test;

public class ParseVersionCopyArgumentTest {

    @Test
    public void parseTrue() throws ParseException {
        for (String argument : Arrays.asList("true", "TRUE", "TrUe")) {
            final Calendar result = Main.parseVersionCopyArgument(argument);
            Assert.assertEquals(0, result.getTimeInMillis());
        }
    }

    @Test
    public void parseDate() throws ParseException {
        final Calendar result = Main.parseVersionCopyArgument("2013-01-01");
        Assert.assertEquals(new GregorianCalendar(2013, 0, 1), result);
    }

    @Test
    public void parseFalse() throws ParseException {
        for (String argument : Arrays.asList("false", "FaLse", "", "xyz", null)) {
            final Calendar result = Main.parseVersionCopyArgument(argument);
            Assert.assertNull(result);
        }
    }
}
