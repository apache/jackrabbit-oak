/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;


import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Random;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;


/*
This can be used as a quick means to get some numbers around elastic query perf during dev cycles 
to compare between old code and new
Disabled by default as these tests don't assert anything and might be time consuming.
To enable perf logs add <logger name="org.apache.jackrabbit.oak.plugins.index.elastic.ElasticPerfTest" level="TRACE"/>
 to logback-test.xml in test resources.
 */
@Ignore
public class ElasticPerfTest extends ElasticAbstractQueryTest {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticPerfTest.class.getName());
    private static final PerfLogger LOG_PERF = new PerfLogger(LOG);
    // Change these to modify the amount of test data created/indexed
    // and the number of times the queries will be executed
    private int NUM_SUB_CONTENT = 500;
    private int NUM_NODES = 500;
    private int NUM_ITERATIONS = 500;
    private String PROP_1 = "foo";
    private String PROP_2 = "title";
    private String PROP_3 = "text";
    private String SAMPLE_TEXT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit";

    @Before
    public void createIndexes() throws Exception {
        IndexDefinitionBuilder builder = createIndex(PROP_1, PROP_2, PROP_3);

        // Change here to add other configurations to these properties if needed
        builder.indexRule("nt:base").property(PROP_3).analyzed();
        builder.indexRule("nt:base").property(PROP_1).analyzed();

        setIndex("testIndex", builder);
        root.commit();

        createTestData();
        // Allow indexing to catch up .
        Thread.sleep(2500);

    }

    // Executes the same query multiple times for NUM_ITERATIONS
    @Test
    public void testFullTextQuery() throws Exception {
        String query = "//*[jcr:contains(@text, 'Lorem')] ";
        long startTest = LOG_PERF.start("Starting test executions");
        for (int j = 0; j < NUM_ITERATIONS; j++) {
            testQuery(query, XPATH);
        }
        LOG_PERF.end(startTest, -1, "{} iterations of tests completed", NUM_ITERATIONS);
    }

    // Executes different queries each time
    @Test
    public void testFullTextQuery2() throws Exception {
        long startTest = LOG_PERF.start("Starting test executions");
        Random rndm = new Random(42);
        for (int j = 0; j < NUM_ITERATIONS; j++) {
            String query = "//*[jcr:contains(@text, 'elit" + rndm.nextInt(NUM_NODES) + "')] ";
            testQuery(query, XPATH);
        }
        LOG_PERF.end(startTest, -1, "{} iterations of tests completed", NUM_ITERATIONS);
    }

    @Test
    public void testPropertyQuery() throws Exception {
        String query = "select [jcr:path] from [nt:base] where [title] = 'Title for node0'";
        long startTest = LOG_PERF.start("Starting test executions");
        for (int j = 0; j < NUM_ITERATIONS; j++) {
            testQuery(query, SQL2);
        }
        LOG_PERF.end(startTest, -1, "{} iterations of tests completed", NUM_ITERATIONS);

    }

    // Executes different queries in the test iterations
    @Test
    public void testPropertyQuery2() throws Exception {
        long startTest = LOG_PERF.start("Starting test executions");
        Random rndm = new Random(42);
        for (int j = 0; j < NUM_ITERATIONS; j++) {
            String query = "select [jcr:path] from [nt:base] where [title] = 'Title for node" + rndm.nextInt(NUM_NODES) + "'";
            testQuery(query, SQL2);
        }
        LOG_PERF.end(startTest, -1, "{} iterations of tests completed", NUM_ITERATIONS);
    }


    private void createTestData() throws Exception {
        Tree content = root.getTree("/").addChild("content");

        for (int i = 0; i < NUM_SUB_CONTENT; i++) {

            Tree subContent = content.addChild("sub_content_" + i);
            subContent.setProperty(PROP_1, "Hello World" + i);
            for (int j = 0; j < NUM_NODES; j++) {
                Tree node = subContent.addChild("node" + j);
                node.setProperty(PROP_2, "Title for node" + j);
                node.setProperty(PROP_3, SAMPLE_TEXT + j);
            }
        }
        root.commit();

    }

    private void testQuery(String query, String language) throws Exception {
        Result result = executeQuery(query, language, NO_BINDINGS);
        Iterable<ResultRow> it = (Iterable<ResultRow>) result.getRows();
        Iterator<ResultRow> iterator = it.iterator();
        long start = LOG_PERF.start("Getting result rows");
        int i = 0;
        while (iterator.hasNext()) {
            i++;
            ResultRow row = iterator.next();
        }
        LOG_PERF.end(start, -1, "{} Results fetched", i);
    }

}
