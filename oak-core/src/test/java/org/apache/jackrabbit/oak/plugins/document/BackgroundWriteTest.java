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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Test;

import static com.google.common.collect.Iterables.all;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.IS_LAST_REV_UPDATE;
import static org.junit.Assert.assertTrue;

/**
 * Tests related to background write operation in DocumentNodeStore.
 */
public class BackgroundWriteTest {

    @Test // OAK-1190
    public void limitMultiUpdate() {
        DocumentMK mk = new DocumentMK.Builder().setDocumentStore(
                new TestStore()).setAsyncDelay(0).open();
        List<String> paths = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; paths.size() < UnsavedModifications.BACKGROUND_MULTI_UPDATE_LIMIT * 2; i++) {
            String child = "node-" + i;
            sb.append("+\"").append(child).append("\":{}");
            paths.add("/" + child);
            for (int j = 0; j < 1000; j++) {
                String p = child + "/node-" + j;
                paths.add("/" + p);
                sb.append("+\"").append(p).append("\":{}");
            }
        }
        mk.commit("/", sb.toString(), null, null);
        mk.runBackgroundOperations();
        Revision r = mk.getNodeStore().newRevision();
        UnsavedModifications pending = mk.getNodeStore().getPendingModifications();
        pending.put("/", r);
        for (String p : paths) {
            pending.put(p, r);
        }
        mk.runBackgroundOperations();
        mk.dispose();
    }

    private static final class TestStore extends MemoryDocumentStore {

        @Override
        public <T extends Document> List<T> createOrUpdate(Collection<T> collection,
                                                           List<UpdateOp> updateOps) {
            if (all(updateOps, IS_LAST_REV_UPDATE)) {
                assertTrue(updateOps.size() <= UnsavedModifications.BACKGROUND_MULTI_UPDATE_LIMIT);
            }
            return super.createOrUpdate(collection, updateOps);
        }
    }
}
