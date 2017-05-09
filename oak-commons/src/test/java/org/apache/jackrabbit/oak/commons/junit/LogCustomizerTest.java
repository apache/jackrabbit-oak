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

package org.apache.jackrabbit.oak.commons.junit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Tests for the LogCustomizer class
 **/
public class LogCustomizerTest {

    private static final Logger LOG = LoggerFactory
            .getLogger(LogCustomizerTest.class);

    @Test
    public void testLogs1() {
        LogCustomizer custom = LogCustomizer
                .forLogger(
                        "org.apache.jackrabbit.oak.commons.junit.LogCustomizerTest")
                .enable(Level.DEBUG).create();

        try {
            custom.starting();
            LOG.debug("test message");
            List<String> logs = custom.getLogs();
            assertTrue(logs.size() == 1);
            assertThat("logs were recorded by custom logger", logs.toString(),
                    containsString("test message"));
        } finally {
            custom.finished();
        }
    }

    @Test
    public void testLogs2() {
        LogCustomizer custom = LogCustomizer
                .forLogger(
                        "org.apache.jackrabbit.oak.commons.junit.LogCustomizerTest")
                .enable(Level.DEBUG).filter(Level.INFO).create();

        try {
            custom.starting();
            LOG.debug("test message");

            List<String> logs = custom.getLogs();
            assertTrue(logs.isEmpty());

        } finally {
            custom.finished();
        }
    }

    @Test
    public void testExactMatch() {
        LogCustomizer custom = LogCustomizer
                .forLogger("org.apache.jackrabbit.oak.commons.junit.LogCustomizerTest")
                .exactlyMatches("Test Message")
                .create();

        try {
            custom.starting();
            LOG.info("test message");
            LOG.info("test message 1");
            LOG.info("1 test message");

            List<String> logs = custom.getLogs();
            assertTrue(logs.isEmpty());

            LOG.info("Test Message");
            assertEquals(1, logs.size());

        } finally {
            custom.finished();
        }
    }


    @Test
    public void testContainsMatch() {
        LogCustomizer custom = LogCustomizer
                .forLogger("org.apache.jackrabbit.oak.commons.junit.LogCustomizerTest")
                .contains("Test Message")
                .create();

        try {
            custom.starting();
            LOG.info("test message");
            LOG.info("test message 1");
            LOG.info("1 test message");

            List<String> logs = custom.getLogs();
            assertTrue(logs.isEmpty());

            LOG.info("Test Message");
            assertEquals(1, logs.size());

            LOG.info("1Test Message");
            LOG.info("1Test Message2");
            LOG.info("1 Test Message");
            assertEquals(4, logs.size());

        } finally {
            custom.finished();
        }
    }


    @Test
    public void testRegexMatch() {
        LogCustomizer custom = LogCustomizer
                .forLogger("org.apache.jackrabbit.oak.commons.junit.LogCustomizerTest")
                .matchesRegex("^Length is [0-9]* units.$")
                .create();

        try {
            custom.starting();
            LOG.info("test message");
            LOG.info("test message 1");
            LOG.info("1 test message");
            LOG.info("1 Length is 10 units.");
            LOG.info("Length is 10 units.1");
            LOG.info("Length is abc units.");

            List<String> logs = custom.getLogs();
            assertTrue(logs.isEmpty());

            LOG.info("Length is 1 units.");
            LOG.info("Length is 20 units.");
            LOG.info("Length is  units.");
            assertEquals(3, logs.size());

        } finally {
            custom.finished();
        }
    }

}
