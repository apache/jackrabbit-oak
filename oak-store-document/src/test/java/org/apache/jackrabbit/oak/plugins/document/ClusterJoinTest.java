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
package org.apache.jackrabbit.oak.plugins.document;

import org.json.simple.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test case for OAK-1170.
 */
public class ClusterJoinTest extends AbstractMongoConnectionTest {

    @Test
    public void nodeJoins() throws Exception {
        String rev1, rev2, rev3;
        // perform manual background ops
        mk.getNodeStore().setAsyncDelay(0);
        rev1 = mk.commit("/", "+\"foo\":{}", null, null);

        // start a new DocumentMK instance. this instance sees /foo
        // because it started after the commit on the first DocumentMK
        DocumentMK mk2 = new DocumentMK.Builder().
                setAsyncDelay(0).
                setMongoDB(connectionFactory.getConnection().getDB()).
                setClusterId(mk.getNodeStore().getClusterId() + 1).
                open();

        try {
            // this creates a first commit from the second DocumentMK instance
            // the first DocumentMK instance does not see this yet
            mk2.commit("/", "+\"bar\":{}+\"bla\":{}", null, null);
            // create a commit on the first DocumentMK. this commit revision
            // is higher than the previous commit on the second DocumentMK
            rev2 = mk.commit("/", "+\"baz\":{}+\"qux\":{}", null, null);
            // @rev1 must only see /foo
            assertChildNodeCount("/", rev1, 1);
            // read children @rev2, should not contain /bar, /bla
            // because there was no background read yet
            JSONObject obj = parseJSONObject(mk.getNodes("/", rev2, 0, 0, 10, null));
            // make changes from second DocumentMK visible
            mk2.runBackgroundOperations();
            mk.runBackgroundOperations();
            // check child nodes of previous getNodes() call
            for (Object key : obj.keySet()) {
                String name = key.toString();
                if (name.startsWith(":")) {
                    continue;
                }
                assertNodesExist(rev2, "/" + name);
            }
            // must only see /foo, /baz and /qux @rev2
            assertEquals(3L, obj.get(":childNodeCount"));
            // @rev3 is after background read
            rev3 = mk.getHeadRevision();
            // now all nodes must be visible
            obj = parseJSONObject(mk.getNodes("/", rev3, 0, 0, 10, null));
            for (Object key : obj.keySet()) {
                String name = key.toString();
                if (name.startsWith(":")) {
                    continue;
                }
                assertNodesExist(rev3, "/" + name);
            }
            // must now see all nodes @rev3
            assertEquals(5L, obj.get(":childNodeCount"));
        } finally {
            mk2.dispose();
        }
    }
}
