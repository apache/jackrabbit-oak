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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedTreeStoreTask;
import org.junit.Test;

public class RemovePropertiesOfBundledNodesTest {

    @Test
    public void cleanUp() {
        // this is similar to the real-world case
        verify("{\"jcr:created\":\"dat:2020-05-06T17:15:13.971Z\",\"jcr:primaryType\":\"nam:nt:file\",\"jcr:createdBy\":\"admin\",\"jcr:content/jcr:lastModified\":\"dat:2025-01-21T03:37:42.095Z\",\"jcr:content/jcr:lastModifiedBy\":\"test\"}",
                "{\"jcr:created\":\"dat:2020-05-06T17:15:13.971Z\",\"jcr:primaryType\":\"nam:nt:file\",\"jcr:createdBy\":\"admin\"}");

        // generic entries
        verify("{}", "{}");
        verify("{\"c\":null,\"b\":\"x\",\"a\":123,\"d\":[1,2,null,\"x\"]}",
                "{\"c\":null,\"b\":\"x\",\"a\":123,\"d\":[1,2,null,\"x\"]}");

        // false positive
        verify("{\"c\":\"jcr:content/that\"}",
                "{\"c\":\"jcr:content/that\"}");

        // generic entries that need cleaning
        verify("{\"c\":null,\"jcr:content/this\":null,\"a\":123,\"jcr:content/that\":[1,2,null,\"x\"]}",
                "{\"c\":null,\"a\":123}");
        verify("{\"c\":null,\"jcr:content/this\":null,\"a\":123,\"array\":[1,2,null,\"x\"]}",
                "{\"c\":null,\"a\":123,\"array\":[1,2,null,\"x\"]}");

    }

    static void verify(String input, String expected) {
        String v2 = PipelinedTreeStoreTask.removePropertiesOfBundledNodes("/test", input);
        assertEquals(expected, v2);

    }
}
