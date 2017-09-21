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
package org.apache.jackrabbit.oak.jcr;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_INCREMENT;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Test;

public class AtomicCounterTest extends AbstractRepositoryTest {        
    public AtomicCounterTest(NodeStoreFixture fixture) {
        super(fixture);
    }
    
    @Test
    public void incrementRootNode() throws RepositoryException {
        
        Session session = getAdminSession();

        try {
            Node root = session.getRootNode();
            Node node = root.addNode("normal node");
            session.save();

            node.setProperty(PROP_INCREMENT, 1L);
            session.save();

            assertTrue("for normal nodes we expect the increment property to be treated as normal",
                node.hasProperty(PROP_INCREMENT));

            node = root.addNode("counterNode");
            node.addMixin(MIX_ATOMIC_COUNTER);
            session.save();

            assertCounter(node, 0);
            
            node.setProperty(PROP_INCREMENT, 1L);
            session.save();
            assertCounter(node, 1);

            // increment again the same node
            node.setProperty(PROP_INCREMENT, 1L);
            session.save();
            assertCounter(node, 2);

            // decrease the counter by 2
            node.setProperty(PROP_INCREMENT, -2L);
            session.save();
            assertCounter(node, 0);

            // increase by 5
            node.setProperty(PROP_INCREMENT, 5L);
            session.save();
            assertCounter(node, 5);
        } finally {
            session.logout();
        }
    }
    
    private static void assertCounter(@Nonnull final Node counter, final long expectedCount) 
                                    throws RepositoryException {
        checkNotNull(counter);
        
        assertTrue(counter.hasProperty(PROP_COUNTER));
        assertEquals(expectedCount, counter.getProperty(PROP_COUNTER).getLong());
        assertFalse(counter.hasProperty(PROP_INCREMENT));
    }
    
    @Test
    public void incrementNonRootNode() throws RepositoryException {
        Session session = getAdminSession();
        
        try {
            Node counter = session.getRootNode().addNode("foo").addNode("bar").addNode("counter");
            counter.addMixin(MIX_ATOMIC_COUNTER);
            session.save();
            
            assertCounter(counter, 0);
            
            counter.setProperty(PROP_INCREMENT, 1L);
            session.save();
            assertCounter(counter, 1);

            // increment again the same node
            counter.setProperty(PROP_INCREMENT, 1L);
            session.save();
            assertCounter(counter, 2);

            // decrease the counter by 2
            counter.setProperty(PROP_INCREMENT, -2L);
            session.save();
            assertCounter(counter, 0);

            // increase by 5
            counter.setProperty(PROP_INCREMENT, 5L);
            session.save();
            assertCounter(counter, 5);
        } finally {
            session.logout();
        }
    }

    @Override
    protected Jcr initJcr(Jcr jcr) {
        return super.initJcr(jcr).withAtomicCounter();
    }
}
