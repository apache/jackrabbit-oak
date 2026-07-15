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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticReliabilitySlowReaderQueryTest extends ElasticAbstractQueryTest {

    private static final String QUERY_PROP_A = "select [jcr:path] from [nt:base] where propa is not null";

    @Override
    protected long getAsyncIteratorEnqueueTimeoutMs() {
        return 1000;
    }

    @Override
    protected long limitReads() {
        return 2;
    }

    /**
     * This tests the case where a reader thread is very slow reading the results and the ElasticResultRowAsyncIterator
     * timeouts out enqueuing results in its internal result queue. To trigger a timeout in the ElasticResultRowAsyncIterator,
     * we set the timeout for the internal queue to 1s and limit the number of results that can be stored in the queue to 2.
     * The following should happen:
     * - the first read will succeed because the reader thread reads it immediately.
     * - The reader thread waits for 2 seconds. During this time, the iterator reads 2 more results and then blocks trying
     * to enqueue the 4th result because the queue is full. After 1 second of waiting, it times out and closes the iterator.
     * - the reader thread awakes up and tries to continue reading. It reads the next two results which were successfully
     * put in the queue before the iterator timed out, even though the iterator is already closed.
     * - Then it fails to read the 4th result and receives an exception indicating that the iterator has timed out.
     */
    @Test
    public void slowReader() throws Exception {
        String indexName = UUID.randomUUID().toString();
        IndexDefinitionBuilder builder = createIndex("propa");
        setIndex(indexName, builder);
        root.commit();

        // Populate the index
        addNodes(6);

        // simulate a slow reader. Reads the first result, waits for 2 seconds,
        Result result = executeQuery(QUERY_PROP_A, SQL2, NO_BINDINGS);
        ArrayList<String> resultPaths = new ArrayList<>();
        Iterator<? extends ResultRow> resultRows = result.getRows().iterator();
        // Read the first result immediately
        assertTrue(resultRows.hasNext());
        resultPaths.add(resultRows.next().getValue(QueryConstants.JCR_PATH).getValue(Type.STRING));
        Thread.sleep(2000L); // Simulate slow reading
        // The iterator should have timed out trying to enqueue the next result. The next two results should still be
        // available because they were enqueued before the queue got full and the iterator timed out.
        assertTrue(resultRows.hasNext());
        resultPaths.add(resultRows.next().getValue(QueryConstants.JCR_PATH).getValue(Type.STRING));
        assertTrue(resultRows.hasNext());
        resultPaths.add(resultRows.next().getValue(QueryConstants.JCR_PATH).getValue(Type.STRING));
        // The next read should fail
        try {
            assertFalse(resultRows.hasNext());
            fail("Expected an exception while reading results");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("Error while fetching results from Elastic"));
        }
        assertEquals(List.of("/test/a0", "/test/a1", "/test/a2"), resultPaths);
    }

    private void addNodes(int count) throws CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        for (int i = 0; i < count; i++) {
            test.addChild("a" + i).setProperty("propa", "a" + i);
        }
        root.commit();
        this.asyncIndexUpdate.run();
        assertEventually(() -> assertQuery(QUERY_PROP_A, List.of("/test/a0", "/test/a1", "/test/a2", "/test/a3", "/test/a4", "/test/a5")));
    }
}
