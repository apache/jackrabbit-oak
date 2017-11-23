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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Strings;

import static junit.framework.Assert.assertNotNull;

/**
 * Test for OAK-1589
 */
public class MongoDocumentStoreLimitsTest extends AbstractMongoConnectionTest {

    @Ignore
    @Test
    public void longName() throws Exception{
        DocumentNodeStore ns = mk.getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();

        builder.child("test");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        String longName = Strings.repeat("foo_", 10000);
        String longPath = String.format("/test/%s", longName);

        builder = ns.getRoot().builder();
        builder.child("test").child(longName);

        try {
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        } catch (CommitFailedException e) {
            // expected to fail
            return;
        }

        // check that the document was created
        // when no exception was thrown
        String id = Utils.getIdFromPath(longPath);
        NodeDocument doc = ns.getDocumentStore().find(Collection.NODES, id, 0);
        assertNotNull(doc);
    }
}
