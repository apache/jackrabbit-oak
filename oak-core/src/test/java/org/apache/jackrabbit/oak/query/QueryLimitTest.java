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

package org.apache.jackrabbit.oak.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.text.ParseException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

public class QueryLimitTest extends AbstractQueryTest {

    MemoryNodeStore store;
    QueryEngineSettings qeSettings;
    private Properties systemProperties;
    int queryLengthWarnLimit = 500;
    int queryLengthErrorLimit = 1000;

    @Override
    protected ContentRepository createRepository() {
        store = new MemoryNodeStore();
        qeSettings = new QueryEngineSettings();

        return new Oak(store)
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(qeSettings)
                .createContentRepository();
    }

    @Before
    @Override
    public void before() throws Exception {
        systemProperties = (Properties) System.getProperties().clone();
        System.setProperty("oak.query.length.warn.limit", "" + queryLengthWarnLimit);
        System.setProperty("oak.query.length.error.limit", "" + queryLengthErrorLimit);
        super.before();
    }

    @After
    public void after() throws Exception {
        System.setProperties(systemProperties);
    }

    @Test
    public void queryLengthErrorLimitBreachThrowsException() throws Exception {
        String generatedString = RandomStringUtils.random(queryLengthErrorLimit, true, false);

        String query = "SELECT [jcr:path] FROM [nt:base] AS a WHERE a.[x]='" + generatedString + "'";
        assertThrows(ParseException.class,
                () -> {
                    try {
                        qe.executeQuery(query, QueryEngineImpl.SQL2, 10, 0,
                                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
                    } catch (RuntimeException e) {
                        Assert.assertEquals("Query length " + query.length() + " is larger than max supported query length: " + queryLengthErrorLimit, e.getMessage());
                        throw e;
                    }
                });
    }

    @Test
    public void queryLengthWarnLimitBreachLogsWarning() throws Exception {
        String generatedString = RandomStringUtils.random(queryLengthWarnLimit, true, false);

        LogCustomizer customLogs = LogCustomizer.forLogger(QueryEngineImpl.class.getName()).enable(Level.WARN).create();

        try {
            customLogs.starting();
            String query = "SELECT [jcr:path] FROM [nt:base] AS a WHERE a.[x]='" + generatedString + "'";

            qe.executeQuery(query, QueryEngineImpl.SQL2, 10, 0,
                    QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);

            String expectedLogMessage = "Query length " + query.length() + " breached queryWarnLimit " + queryLengthWarnLimit + ". Query: " + query;

            Assert.assertThat(customLogs.getLogs(), IsCollectionContaining.hasItems(expectedLogMessage));
        } finally {
            customLogs.finished();
        }
    }

    @Test
    public void queryLimitFromOptions() throws Exception {
        String query = "SELECT [jcr:path] FROM [nt:base] AS a OPTION(LIMIT 10)";

        Iterator<? extends ResultRow> row = qe.executeQuery(query, QueryEngineImpl.SQL2, Optional.empty(),
                Optional.empty(), Collections.emptyMap(), Collections.emptyMap()).getRows().iterator();
        int count = 0;
        while (row.hasNext()) {
            count++;
            row.next();
        }
        assertEquals(10, count);
    }
}
