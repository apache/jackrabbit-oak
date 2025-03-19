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
package org.apache.jackrabbit.oak.commons.properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class SystemPropertySupplierTest {

    private static final Logger LOG = LoggerFactory.getLogger(SystemPropertySupplierTest.class);

    @Test
    public void testBoolean() {
        assertEquals(Boolean.TRUE, SystemPropertySupplier.create("foo", Boolean.TRUE).usingSystemPropertyReader((n) -> null).get());
        assertEquals(Boolean.FALSE,
                SystemPropertySupplier.create("foo", Boolean.FALSE).usingSystemPropertyReader((n) -> null).get());
        assertEquals(Boolean.TRUE,
                SystemPropertySupplier.create("foo", Boolean.FALSE).usingSystemPropertyReader((n) -> "true").get());
        assertEquals(Boolean.FALSE,
                SystemPropertySupplier.create("foo", Boolean.FALSE).usingSystemPropertyReader((n) -> "false").get());
    }

    @Test
    public void testInteger() {
        assertEquals(Integer.valueOf(123),
                SystemPropertySupplier.create("foo", Integer.valueOf(123)).usingSystemPropertyReader((n) -> null).get());
        assertEquals(Integer.valueOf(1742),
                SystemPropertySupplier.create("foo", Integer.valueOf(123)).usingSystemPropertyReader((n) -> "1742").get());
    }

    @Test
    public void testLong() {
        long big = Long.MAX_VALUE;
        assertEquals(Long.valueOf(big),
                SystemPropertySupplier.create("foo", Long.valueOf(big)).usingSystemPropertyReader((n) -> null).get());
        assertEquals(Long.valueOf(1742),
                SystemPropertySupplier.create("foo", Long.valueOf(big)).usingSystemPropertyReader((n) -> "1742").get());
    }

    @Test
    public void testString() {
        assertEquals("bar", SystemPropertySupplier.create("foo", "bar").usingSystemPropertyReader((n) -> null).get());
        assertEquals("", SystemPropertySupplier.create("foo", "bar").usingSystemPropertyReader((n) -> "").get());
    }

    @Test
    public void testFilter() {
        LogCustomizer logCustomizer = LogCustomizer.forLogger(SystemPropertySupplierTest.class.getName()).enable(Level.ERROR)
                .contains("Ignoring invalid value").create();
        logCustomizer.starting();

        try {
            int positive = SystemPropertySupplier.create("foo", Integer.valueOf(123)).loggingTo(LOG)
                    .usingSystemPropertyReader((n) -> "-1").validateWith(n -> n >= 0).get();
            assertEquals(123, positive);
            assertEquals(1, logCustomizer.getLogs().size());
        } finally {
            logCustomizer.finished();
        }
    }

    @Test
    public void testHidden() {
        String secret = "secret123";
        LogCustomizer logCustomizer = LogCustomizer.forLogger(SystemPropertySupplierTest.class.getName()).enable(Level.TRACE)
                .contains(secret).create();
        logCustomizer.starting();

        try {
            String password = SystemPropertySupplier.create("password", "").hideValue().loggingTo(LOG)
                    .usingSystemPropertyReader((n) -> secret).get();
            assertEquals(secret, password);
            assertTrue("Log message should not contain secret password, but found: " + logCustomizer.getLogs(),
                    logCustomizer.getLogs().size() == 0);
        } finally {
            logCustomizer.finished();
        }
    }

    @Test
    public void testNonParseable() {
        LogCustomizer logCustomizer = LogCustomizer.forLogger(SystemPropertySupplierTest.class.getName()).enable(Level.ERROR)
                .contains("Ignoring malformed value").create();
        logCustomizer.starting();

        try {
            int positive = SystemPropertySupplier.create("foo", Integer.valueOf(123)).loggingTo(LOG)
                    .usingSystemPropertyReader((n) -> "abc").validateWith(n -> n >= 0).get();
            assertEquals(123, positive);
            assertEquals(1, logCustomizer.getLogs().size());
        } finally {
            logCustomizer.finished();
        }
    }

    @Test
    public void testRedirectSuccess() {
        Logger local = LoggerFactory.getLogger(SystemPropertySupplierTest.class.getName() + "-FOO");
        LogCustomizer logCustomizer = LogCustomizer.forLogger(SystemPropertySupplierTest.class.getName() + "-FOO")
                .enable(Level.TRACE).create();
        logCustomizer.starting();

        try {
            int positive = SystemPropertySupplier.create("foo", Integer.valueOf(123)).loggingTo(local)
                    .usingSystemPropertyReader((n) -> "4217").get();
            assertEquals(4217, positive);
            assertEquals(2, logCustomizer.getLogs().size());
        } finally {
            logCustomizer.finished();
        }
    }

    @Test
    public void testUnsupportedType() {
        try {
            SystemPropertySupplier.create("foo", new Object());
        } catch (IllegalArgumentException expected) {
        }
    }
}
