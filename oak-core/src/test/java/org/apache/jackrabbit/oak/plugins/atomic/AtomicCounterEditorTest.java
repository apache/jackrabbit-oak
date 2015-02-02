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
package org.apache.jackrabbit.oak.plugins.atomic;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PREFIX_PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_INCREMENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Ignore;
import org.junit.Test;

public class AtomicCounterEditorTest {
    @Test
    @Ignore // FIXME fix test expectations
    public void childNodeAdded() throws CommitFailedException {
        NodeBuilder builder = EMPTY_NODE.builder();
        
        Editor editor = new AtomicCounterEditor(EMPTY_NODE.builder());
        
        assertNull("without the mixin we should not process",
            editor.childNodeAdded("foo", builder.getNodeState()));
        
        builder = EMPTY_NODE.builder();
        builder = setMixin(builder);
        assertTrue("with the mixin set we should get a proper Editor",
            editor.childNodeAdded("foo", builder.getNodeState()) instanceof AtomicCounterEditor);
    }
    
    @Test
    public void increment() throws CommitFailedException {
        NodeBuilder builder;
        Editor editor;
        PropertyState property;
        
        builder = EMPTY_NODE.builder();
        editor = new AtomicCounterEditor(builder);
        property = PropertyStates.createProperty(PROP_INCREMENT, 1L, Type.LONG);
        editor.propertyAdded(property);
        assertNoCounters(builder.getProperties());
        
        builder = EMPTY_NODE.builder();
        builder = setMixin(builder);
        editor = new AtomicCounterEditor(builder);
        property = PropertyStates.createProperty(PROP_INCREMENT, 1L, Type.LONG);
        editor.propertyAdded(property);
        assertNull("the oak:increment should never be set", builder.getProperty(PROP_INCREMENT));
        assertTotalCounters(builder.getProperties(), 1);
    }
    
    @Test
    public void consolidate() throws CommitFailedException {
        NodeBuilder builder;
        Editor editor;
        PropertyState property;
        
        builder = EMPTY_NODE.builder();
        builder = setMixin(builder);
        editor = new AtomicCounterEditor(builder);
        property = PropertyStates.createProperty(PROP_INCREMENT, 1L, Type.LONG);
        
        editor.propertyAdded(property);
        assertTotalCounters(builder.getProperties(), 1);
        editor.propertyAdded(property);
        assertTotalCounters(builder.getProperties(), 2);
        AtomicCounterEditor.consolidateCount(builder);
        assertNotNull(builder.getProperty(PROP_COUNTER));
        assertEquals(2, builder.getProperty(PROP_COUNTER).getValue(LONG).longValue());
        assertNoCounters(builder.getProperties());
    }

    /**
     * that a list of properties does not contains any property with name starting with
     * {@link AtomicCounterEditor#PREFIX_PROP_COUNTER}
     * 
     * @param properties
     */
    private static void assertNoCounters(@Nonnull final Iterable<? extends PropertyState> properties) {
        checkNotNull(properties);
        
        for (PropertyState p : properties) {
            assertFalse("there should be no counter property",
                p.getName().startsWith(PREFIX_PROP_COUNTER));
        }
    }
    
    /**
     * assert the total amount of {@link AtomicCounterEditor#PREFIX_PROP_COUNTER}
     * 
     * @param properties
     */
    private static void assertTotalCounters(@Nonnull final Iterable<? extends PropertyState> properties,
                                            int expected) {
        int total = 0;
        for (PropertyState p : checkNotNull(properties)) {
            if (p.getName().startsWith(PREFIX_PROP_COUNTER)) {
                total += p.getValue(LONG);
            }
        }
        
        assertEquals("the total amount of :oak-counter properties does not match", expected, total);
    }
    
    private static NodeBuilder setMixin(@Nonnull final NodeBuilder builder) {
        return checkNotNull(builder).setProperty(JCR_MIXINTYPES, of(MIX_ATOMIC_COUNTER), NAMES);
    }
}
