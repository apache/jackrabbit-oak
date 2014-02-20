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
package org.apache.jackrabbit.mk.persistence;

import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.MutableCommit;
import org.apache.jackrabbit.mk.model.MutableNode;
import org.apache.jackrabbit.mk.model.StoredNode;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests verifying the contract provided by {@link GCPersistence}.
 */
@RunWith(value = Parameterized.class)
public class GCPersistenceTest {

    private Class<GCPersistence> pmClass;
    private GCPersistence pm;
    
    public GCPersistenceTest(Class<GCPersistence> pmClass) {
        this.pmClass = pmClass;
    }
    
    @Before
    public void setup() throws Exception {
        pm = pmClass.newInstance();
        pm.initialize(new File("target/mk"));
        
        // start with empty repository
        pm.start();
        pm.sweep();
    }
    
    @SuppressWarnings("rawtypes")
    @Parameters
    public static Collection classes() {
        Class[][] pmClasses = new Class[][] {
                { H2Persistence.class },
                { InMemPersistence.class }
        };
        return Arrays.asList(pmClasses);
    }
    
    @After
    public void tearDown() throws Exception {
        IOUtils.closeQuietly(pm);
    }

    @Test
    public void testOldNodeIsSwept() throws Exception {
        MutableNode node = new MutableNode(null);
        Id id = pm.writeNode(node);

        Thread.sleep(1);
        pm.start();
        pm.sweep();
        
        try {
            pm.readNode(new StoredNode(id, null));
            fail();
        } catch (NotFoundException e) {
            /* expected */
        }
    }

    @Test
    public void testMarkedNodeIsNotSwept() throws Exception {
        MutableNode node = new MutableNode(null);
        Id id = pm.writeNode(node);

        // small delay needed
        Thread.sleep(100);
        
        pm.start();

        // old node must not be marked 
        assertTrue(pm.markNode(id));
        
        pm.sweep();
        pm.readNode(new StoredNode(id, null));
    }
    
    @Test
    public void testNewNodeIsNotSwept() throws Exception {
        pm.start();
        
        MutableNode node = new MutableNode(null);
        Id id = pm.writeNode(node);
        
        // new node must already be marked 
        assertFalse(pm.markNode(id));

        pm.sweep();
        pm.readNode(new StoredNode(id, null));
    }
    
    @Test
    public void testReplaceCommit() throws Exception {
        MutableCommit c1 = new MutableCommit();
        c1.setRootNodeId(Id.fromLong(0));
        pm.writeCommit(Id.fromLong(1), c1);

        MutableCommit c2 = new MutableCommit();
        c2.setParentId(c1.getId());
        c2.setRootNodeId(Id.fromLong(0));
        pm.writeCommit(Id.fromLong(2), c2);

        pm.start();
        c2 = new MutableCommit();
        c2.setRootNodeId(Id.fromLong(0));
        pm.replaceCommit(Id.fromLong(2), c2);
        pm.sweep();
        
        assertEquals(null, pm.readCommit(Id.fromLong(2)).getParentId());
    }
}

