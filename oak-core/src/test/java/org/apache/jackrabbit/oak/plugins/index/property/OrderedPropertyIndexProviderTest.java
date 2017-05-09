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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import ch.qos.logback.classic.Level;

public class OrderedPropertyIndexProviderTest extends AbstractQueryTest {
    private final LogCustomizer custom = LogCustomizer
        .forLogger(OrderedPropertyIndexProvider.class.getName()).enable(Level.WARN).create();
    
    @Override
    protected void createTestIndexNode() throws Exception {
        // no index definitions OOTB
    }
    
    @Override
    protected ContentRepository createRepository() {
        return new Oak()
            .with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new ReferenceIndexProvider())
            .with(new OrderedPropertyIndexProvider())
            .createContentRepository();
    }
    
    @Test
    public void singleQueryRun() {
        custom.starting();
        executeQuery("SELECT * FROM [oak:Unstructured]", SQL2);
        List<String> logs = custom.getLogs();
        assertEquals(1, logs.size());
        assertThat(logs, hasItem(OrderedIndex.DEPRECATION_MESSAGE));
        custom.finished();
    }
    
    @Test
    public void multipleQueryRuns() {
        final int executions = 16;
        final int trackEvery = 5;
        final int numTraces = executions / trackEvery;
        OrderedPropertyIndexProvider.setThreshold(trackEvery);
        List<String> expectedLogs = Collections.nCopies(numTraces, OrderedIndex.DEPRECATION_MESSAGE);
        custom.starting();
        for (int i = 0; i < executions; i++) {
            executeQuery("SELECT * FROM [oak:Unstructured]", SQL2);
        }
        assertThat(custom.getLogs(), is(expectedLogs));
        custom.finished();
    }
}
