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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.jackrabbit.oak.commons.junit.TemporarySystemProperty;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.Session;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@code WarnLogStringPropertySizeTest} checks if Warn log is bein added on adding
 * large string properties
 */
@RunWith(Parameterized.class)
public class WarnLogStringPropertySizeTest extends AbstractRepositoryTest {

    @Rule
    public TemporarySystemProperty temporarySystemProperty = new TemporarySystemProperty();

    private final static String testStringPropertyKey = "testStringPropertyKey";
    private final static String testLargeStringPropertyValue = "abcdefghijklmnopqrstuvwxyz";
    private final static String testSmallStringPropertyValue = "abcd";
    private final static String nodeImplLogger = NodeImpl.class.getName();
    private final static String warnMessage = "String length: {} for property: {} at Node: {} is greater than configured value {}";
    private static ListAppender<ILoggingEvent> listAppender = null;

    public WarnLogStringPropertySizeTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void loggingAppenderStart() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        listAppender = new ListAppender<>();
        listAppender.start();
        context.getLogger(nodeImplLogger).addAppender(listAppender);
    }

    @After
    public void loggingAppenderStop() {
        listAppender.stop();
    }

    @Test
    public void noWarnLogOnAddingSmallStringProperties() throws Exception {
        Session s = getAdminSession();
        Node test = s.getRootNode().addNode("testSmall");
        test.setProperty(testStringPropertyKey, testSmallStringPropertyValue);
        assertFalse(isWarnMessagePresent(listAppender));
    }

    @Test
    public void warnLogOnAddingLargeStringPropertiesWithCustomThreshold() throws Exception {
        System.setProperty(OakJcrConstants.WARN_LOG_STRING_SIZE_THRESHOLD_KEY, "10");
        Session s = getAdminSession();
        Node test = s.getRootNode().addNode("testLarge");
        test.setProperty(testStringPropertyKey, testLargeStringPropertyValue);
        assertTrue(isWarnMessagePresent(listAppender));
    }

    private boolean isWarnMessagePresent(ListAppender<ILoggingEvent> listAppender) {
        for (ILoggingEvent loggingEvent : listAppender.list) {
            if (loggingEvent.getMessage().contains(warnMessage)) {
                return true;
            }
        }
        return false;
    }
}
