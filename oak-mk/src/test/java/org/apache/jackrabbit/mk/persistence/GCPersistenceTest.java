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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.MutableNode;
import org.apache.jackrabbit.mk.model.StoredNode;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.apache.jackrabbit.mk.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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
        MutableNode node = new MutableNode(null, "/");
        Id id = pm.writeNode(node);

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
        MutableNode node = new MutableNode(null, "/");
        Id id = pm.writeNode(node);

        pm.start();
        pm.markNode(id);
        pm.sweep();
        
        pm.readNode(new StoredNode(id, null));
    }

    @Test
    public void testOldNodeIsUnmarked() throws Exception {
        MutableNode node = new MutableNode(null, "/");
        Id id = pm.writeNode(node);
        
        // small delay needed
        Thread.sleep(100);
        
        pm.start();
        assertTrue(pm.markNode(id));
    }
    
    @Test
    public void testNewNodeIsMarked() throws Exception {
        pm.start();
        
        MutableNode node = new MutableNode(null, "/");
        Id id = pm.writeNode(node);
        
        assertFalse(pm.markNode(id));
    }

    @Test
    public void testNewNodeIsNotSwept() throws Exception {
        pm.start();
        
        MutableNode node = new MutableNode(null, "/");
        Id id = pm.writeNode(node);
        
        pm.sweep();
        pm.readNode(new StoredNode(id, null));
    }
}

