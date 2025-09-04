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

import org.junit.Test;

public class SessionCacheTest {

    @Test
    public void test() {
        Store store = StoreBuilder.build("type:memory\n" +
                Store.MAX_FILE_SIZE_BYTES + "=1000\n" +
                TreeSession.CACHE_SIZE_MB + "=1");
        TreeSession s = new TreeSession(store);
        s.init();
        for (int i = 0; i < 50_000; i++) {
            s.put("k" + i, "v" + i);
        }
        s.flush();
        assertEquals("root #0 contains 1756 files (file name root)\n"
                + "cache entries:1049 max:1048576 used:1049000", s.getInfo());
    }
}
