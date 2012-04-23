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
package org.apache.jackrabbit.mk.store;

import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.core.Repository;
import org.apache.jackrabbit.mk.json.fast.Jsop;
import org.apache.jackrabbit.mk.json.fast.JsopArray;
import org.apache.jackrabbit.mk.model.MutableCommit;
import org.apache.jackrabbit.mk.persistence.InMemPersistence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests verifying the inner workings of <code>DefaultRevisionStore</code>.
 */
public class DefaultRevisionStoreTest {

    private DefaultRevisionStore rs;
    private MicroKernel mk;
    
    @Before
    public void setup() throws Exception {
        rs = new DefaultRevisionStore(new InMemPersistence()) {
            @Override
            protected void doMark() throws Exception {
                // Keep head commit only
                markCommit(getHeadCommit());
                
                MutableCommit headCommit = new MutableCommit(getHeadCommit());  
                headCommit.setParentId(null);
                gcpm.replaceCommit(headCommit.getId(), headCommit);
            }
        };
        rs.initialize();
        mk = new MicroKernelImpl(new Repository(rs, new MemoryBlobStore()));
    }

    @After
    public void tearDown() throws Exception {
        if (mk != null) {
            mk.dispose();
        }
    }
    
    @Test
    public void testGC() {
        mk.commit("/", "+\"a\" : { \"c\":{}, \"d\":{} }", mk.getHeadRevision(), null);
        mk.commit("/", "+\"b\" : {}", mk.getHeadRevision(), null);
        mk.commit("/b", "+\"e\" : {}", mk.getHeadRevision(), null);
        mk.commit("/a/c", "+\"f\" : {}", mk.getHeadRevision(), null);
        
        String headRevision = mk.getHeadRevision();
        String contents = mk.getNodes("/", headRevision);

        rs.gc();
        
        assertEquals(headRevision, mk.getHeadRevision());
        assertEquals(contents, mk.getNodes("/", headRevision));
        
        String history = mk.getRevisionHistory(Long.MIN_VALUE, Integer.MIN_VALUE);
        assertEquals(1, ((JsopArray) Jsop.parse(history)).size());
    }
}
