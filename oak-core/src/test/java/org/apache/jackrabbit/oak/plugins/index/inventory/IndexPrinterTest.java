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
package org.apache.jackrabbit.oak.plugins.index.inventory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoService;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexPrinterTest {
    private final AsyncIndexInfoService asyncInfo = mock(AsyncIndexInfoService.class);
    private final IndexInfoService indexInfo = mock(IndexInfoService.class);

    private final IndexPrinter printer = new IndexPrinter(indexInfo, asyncInfo);

    @Test
    public void asyncIndexInfo() {
        when(indexInfo.getAllIndexInfo()).thenReturn(emptyList());
        when(asyncInfo.getAsyncLanes()).thenReturn(asList("foo-async", "bar-async"));
        when(asyncInfo.getInfo("foo-async"))
                .thenReturn(new AsyncIndexInfo("foo-async", 0, 0, false, null));

        String output = getPrintOutput(Format.TEXT);

        assertThat(output, containsString("foo-async"));
    }

    @Test
    public void asyncIndexInfoJson() {
        when(indexInfo.getAllIndexInfo()).thenReturn(emptyList());
        when(asyncInfo.getAsyncLanes()).thenReturn(asList("foo-async", "bar-async"));
        when(asyncInfo.getInfo("foo-async"))
                .thenReturn(new AsyncIndexInfo("foo-async", 0, 0, false, null));

        String output = getPrintOutput(Format.JSON);

        JsonObject json = JsonObject.fromJson(output, true);
        Map<String, JsonObject> jsonMap = json.getChildren();
        assertTrue(jsonMap.containsKey("Async Indexers State"));
        int size = Integer.parseInt(jsonMap.get("Async Indexers State").getProperties().get("Number of async indexer lanes"));
        assertEquals(2, size);

        assertTrue(jsonMap.get("Async Indexers State").getChildren().containsKey("foo-async"));
        assertTrue(jsonMap.get("Async Indexers State").getChildren().containsKey("bar-async"));
    }

    @Test
    public void indexInfo() {
        when(asyncInfo.getAsyncLanes()).thenReturn(emptyList());

        TestInfo info1 = new TestInfo("/oak:index/fooIndex", "property");
        TestInfo info2 = new TestInfo("/oak:index/barIndex", "lucene");
        info2.laneName = "async";

        when(indexInfo.getAllIndexInfo()).thenReturn(asList(info1, info2));

        String output = getPrintOutput(Format.TEXT);
        assertThat(output, containsString("/oak:index/fooIndex"));
        assertThat(output, containsString("/oak:index/barIndex"));
        assertThat(output, containsString("async"));
    }

    @Test
    public void indexInfoJson() {
        when(asyncInfo.getAsyncLanes()).thenReturn(emptyList());

        TestInfo info1 = new TestInfo("/oak:index/fooIndex", "property");
        TestInfo info2 = new TestInfo("/oak:index/barIndex", "lucene");
        info2.laneName = "async";

        when(indexInfo.getAllIndexInfo()).thenReturn(asList(info1, info2));

        String output = getPrintOutput(Format.JSON);
        JsonObject json = JsonObject.fromJson(output, true);
        Map<String, JsonObject> jsonMap = json.getChildren();
        assertTrue(jsonMap.containsKey("Async Indexers State"));

        assertEquals(0, jsonMap.get("Async Indexers State").getChildren().size());

        assertTrue(jsonMap.containsKey("lucene"));
        assertTrue(jsonMap.containsKey("property"));
        assertTrue(jsonMap.get("lucene").getChildren().containsKey("/oak:index/barIndex"));
        assertFalse(jsonMap.get("lucene").getChildren().containsKey("/oak:index/fooIndex"));
        assertTrue(jsonMap.get("property").getChildren().containsKey("/oak:index/fooIndex"));
        assertFalse(jsonMap.get("property").getChildren().containsKey("/oak:index/barIndex"));
    }

    private String getPrintOutput(Format format) {
        StringWriter sw = new StringWriter();
        printer.print(new PrintWriter(sw), format, false);
        return sw.toString();
    }

    private static class TestInfo implements IndexInfo {
        final String indexPath;
        final String type;
        String laneName;

        private TestInfo(String indexPath, String type) {
            this.indexPath = indexPath;
            this.type = type;
        }

        @Override
        public String getIndexPath() {
            return indexPath;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getAsyncLaneName() {
            return null;
        }

        @Override
        public long getLastUpdatedTime() {
            return 0;
        }

        @Override
        public long getIndexedUpToTime() {
            return 0;
        }

        @Override
        public long getEstimatedEntryCount() {
            return 0;
        }

        @Override
        public long getSizeInBytes() {
            return 0;
        }

        @Override
        public boolean hasIndexDefinitionChangedWithoutReindexing() {
            return false;
        }

        @Override
        public String getIndexDefinitionDiff() {
            return null;
        }

        @Override
        public boolean hasHiddenOakLibsMount() {
            return false;
        }

        @Override
        public boolean hasPropertyIndexNode() {
            return false;
        }

        @Override
        public long getSuggestSizeInBytes() {
            return 0;
        }

        @Override
        public long getCreationTimestamp() {
            return 0;
        }

        @Override
        public long getReindexCompletionTimestamp() {
            return 0;
        }
    }

}
