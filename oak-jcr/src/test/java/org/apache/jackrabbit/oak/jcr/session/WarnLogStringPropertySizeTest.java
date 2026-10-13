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
package org.apache.jackrabbit.oak.jcr.session;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.commons.junit.TemporarySystemProperty;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.event.Level;

import javax.jcr.Node;
import javax.jcr.Session;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@code WarnLogStringPropertySizeTest} checks if WARN log is being added on adding
 * large string properties
 */
@RunWith(Parameterized.class)
public class WarnLogStringPropertySizeTest extends AbstractRepositoryTest {

    @Rule
    public TemporarySystemProperty temporarySystemProperty = new TemporarySystemProperty();

    private final static String testStringPropertyKey = "testStringPropertyKey";
    private final static String testLargeStringPropertyValue = "a".repeat(OakJcrConstants.DEFAULT_WARN_LOG_STRING_SIZE_THRESHOLD_VALUE + 1);
    private final static String testSmallStringPropertyValue = "a".repeat(OakJcrConstants.DEFAULT_WARN_LOG_STRING_SIZE_THRESHOLD_VALUE);
    private final static String warnMessage = "String length: .* for property: .* at Node: .* is greater than configured value .*";
    private final LogCustomizer logger;

    public WarnLogStringPropertySizeTest(NodeStoreFixture fixture) {
        super(fixture);
        logger = LogCustomizer.forLogger(NodeImpl.class).enable(Level.WARN).matchesRegex(warnMessage).create();
    }

    @Before
    public void loggingAppenderStart() {
        logger.starting();
    }

    @After
    public void loggingAppenderStop() {
        logger.finished();
    }

    @Test
    public void noWarnLogOnAddingSmallStringProperties() throws Exception {
        Session s = getAdminSession();
        Node test = s.getRootNode().addNode("testSmall");
        test.setProperty(testStringPropertyKey, testSmallStringPropertyValue);
        assertFalse(isWarnMessagePresent(logger));
    }

    @Test
    public void warnLogOnAddingLargeStringPropertiesWithCustomThreshold() throws Exception {
        Session s = getAdminSession();
        Node test = s.getRootNode().addNode("testLarge");
        test.setProperty(testStringPropertyKey, testLargeStringPropertyValue);
        assertTrue(logger.getLogs().toString(), isWarnMessagePresent(logger));
    }

    private boolean isWarnMessagePresent(LogCustomizer logger) {
        return !logger.getLogs().isEmpty();
    }
}
