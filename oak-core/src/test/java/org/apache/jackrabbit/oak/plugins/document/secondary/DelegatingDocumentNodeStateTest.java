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

package org.apache.jackrabbit.oak.plugins.document.secondary;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.secondary.DelegatingDocumentNodeState.PROP_LAST_REV;
import static org.apache.jackrabbit.oak.plugins.document.secondary.DelegatingDocumentNodeState.PROP_REVISION;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DelegatingDocumentNodeStateTest {
    private NodeBuilder builder = EMPTY_NODE.builder();

    @Test
    public void basicWorking() throws Exception{
        RevisionVector rv1 = new RevisionVector(new Revision(1,0,1));
        RevisionVector rv2 = new RevisionVector(new Revision(1,0,3));
        builder.setProperty(asPropertyState(PROP_REVISION, rv1));
        builder.setProperty(asPropertyState(PROP_LAST_REV, rv2));
        AbstractDocumentNodeState state = DelegatingDocumentNodeState.wrap(builder.getNodeState(), NodeStateDiffer.DEFAULT_DIFFER);

        assertEquals(rv1, state.getRootRevision());
        assertEquals(rv2, state.getLastRevision());
        assertTrue(state.hasNoChildren());
        assertTrue(state.exists());
        assertFalse(state.isFromExternalChange());
    }
    
    @Test
    public void metaPropertiesFilteredOut() throws Exception{
        setMetaProps(builder);
        builder.setProperty("foo", "bar");

        AbstractDocumentNodeState state = DelegatingDocumentNodeState.wrap(builder.getNodeState(), NodeStateDiffer.DEFAULT_DIFFER);
        assertEquals(1, Iterables.size(state.getProperties()));
        assertEquals(1, state.getPropertyCount());
    }

    @Test
    public void childNodeDecorated() throws Exception{
        setMetaProps(builder);
        setMetaProps(builder.child("a"));
        setMetaProps(builder.child("b"));

        AbstractDocumentNodeState state = DelegatingDocumentNodeState.wrap(builder.getNodeState(), NodeStateDiffer.DEFAULT_DIFFER);
        assertTrue(state.getChildNode("a") instanceof AbstractDocumentNodeState);
        assertTrue(state.getChildNode("b") instanceof AbstractDocumentNodeState);
        assertFalse(state.hasChildNode("c"));
        assertFalse(state.getChildNode("c").exists());

        assertFalse(state.hasNoChildren());

        for(ChildNodeEntry cne : state.getChildNodeEntries()){
            assertTrue(cne.getNodeState() instanceof AbstractDocumentNodeState);
        }

        assertEquals(2, state.getChildNodeCount(100));
    }

    @Test
    public void withRootRevision() throws Exception{
        RevisionVector rv1 = new RevisionVector(new Revision(1,0,1));
        RevisionVector rv2 = new RevisionVector(new Revision(1,0,3));
        builder.setProperty(asPropertyState(PROP_REVISION, rv1));
        builder.setProperty(asPropertyState(PROP_LAST_REV, rv2));
        AbstractDocumentNodeState state = DelegatingDocumentNodeState.wrap(builder.getNodeState(), NodeStateDiffer.DEFAULT_DIFFER);

        AbstractDocumentNodeState state2 = state.withRootRevision(rv1, false);
        assertSame(state, state2);

        RevisionVector rv4 = new RevisionVector(new Revision(1,0,4));
        AbstractDocumentNodeState state3 = state.withRootRevision(rv4, true);
        assertEquals(rv4, state3.getRootRevision());
        assertTrue(state3.isFromExternalChange());
    }

    @Test
    public void wrapIfPossible() throws Exception{
        assertFalse(DelegatingDocumentNodeState.wrapIfPossible(EMPTY_NODE, NodeStateDiffer.DEFAULT_DIFFER)
                instanceof AbstractDocumentNodeState);

        setMetaProps(builder);
        assertTrue(DelegatingDocumentNodeState.wrapIfPossible(builder.getNodeState(), NodeStateDiffer.DEFAULT_DIFFER) instanceof
                AbstractDocumentNodeState);
    }

    @Test
    public void equals1() throws Exception{
        setMetaProps(builder);
        builder.setProperty("foo", "bar");

        NodeBuilder b2 = EMPTY_NODE.builder();
        b2.setProperty("foo", "bar");

        assertTrue(EqualsDiff.equals(DelegatingDocumentNodeState.wrap(builder.getNodeState(), NodeStateDiffer.DEFAULT_DIFFER),
                b2.getNodeState()));
        assertTrue(EqualsDiff.equals(b2.getNodeState(),
                DelegatingDocumentNodeState.wrap(builder.getNodeState(), NodeStateDiffer.DEFAULT_DIFFER)));
    }

    private static void setMetaProps(NodeBuilder nb){
        nb.setProperty(asPropertyState(PROP_REVISION, new RevisionVector(new Revision(1,0,1))));
        nb.setProperty(asPropertyState(PROP_LAST_REV, new RevisionVector(new Revision(1,0,1))));
    }

    private static PropertyState asPropertyState(String name, RevisionVector revision) {
        return createProperty(name, revision.asString());
    }

}