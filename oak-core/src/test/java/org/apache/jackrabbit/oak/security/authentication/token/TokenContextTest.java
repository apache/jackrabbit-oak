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
package org.apache.jackrabbit.oak.security.authentication.token;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants.TOKENS_NODE_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TokenContextTest {

    private final Context ctx = TokenContext.getInstance();

    @Test
    public void testDefinesProperty() {
        PropertyState ps = mock(PropertyState.class);
        Tree t = when(mock(Tree.class).getName()).thenReturn(TOKENS_NODE_NAME).getMock();

        assertFalse(ctx.definesProperty(t, ps));

        when(t.getName()).thenReturn("anyName");
        PropertyState primaryType = PropertyStates.createProperty(JCR_PRIMARYTYPE, TokenConstants.TOKEN_NT_NAME, Type.NAME);
        when(t.getProperty(JCR_PRIMARYTYPE)).thenReturn(primaryType);
        
        assertTrue(ctx.definesProperty(t, ps));

        PropertyState primaryType2 = PropertyStates.createProperty(JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        when(t.getProperty(JCR_PRIMARYTYPE)).thenReturn(primaryType2);

        assertFalse(ctx.definesProperty(t, primaryType2));
        
        verifyNoInteractions(ps);
        verify(t, never()).getName();
        verify(t, times(3)).getProperty(JCR_PRIMARYTYPE);
        verifyNoMoreInteractions(t);
    }
    
    @Test
    public void testDefinesContextRoot() {
        Tree t = when(mock(Tree.class).getName()).thenReturn(TOKENS_NODE_NAME).getMock();
        assertTrue(ctx.definesContextRoot(t));
        
        when(t.getName()).thenReturn("anyName", TokenConstants.TOKEN_ATTRIBUTE, TokenConstants.TOKEN_ATTRIBUTE_KEY);
        assertFalse(ctx.definesContextRoot(t));
        assertFalse(ctx.definesContextRoot(t));
        assertFalse(ctx.definesContextRoot(t));
        
        verify(t, times(4)).getName();
        verifyNoMoreInteractions(t);
    }
    
    @Test
    public void testDefinesTree() {
        Tree t = when(mock(Tree.class).getName()).thenReturn(TOKENS_NODE_NAME).getMock();
        assertTrue(ctx.definesTree(t));

        when(t.getName()).thenReturn("anyName");
        PropertyState ps = PropertyStates.createProperty(JCR_PRIMARYTYPE, TokenConstants.TOKEN_NT_NAME, Type.NAME);
        when(t.getProperty(JCR_PRIMARYTYPE)).thenReturn(ps);

        assertTrue(ctx.definesTree(t));

        PropertyState ps2 = PropertyStates.createProperty(JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        when(t.getProperty(JCR_PRIMARYTYPE)).thenReturn(ps2);

        assertFalse(ctx.definesTree(t));

        verify(t, times(3)).getName();
        verify(t, times(2)).getProperty(anyString());
        
        verifyNoMoreInteractions(t);
    }
    
    @Test
    public void testDefinesLocation() {
        PropertyState ps = mock(PropertyState.class);

        TreeLocation tl = mock(TreeLocation.class);
        when(tl.getName()).thenReturn(TOKENS_NODE_NAME);
        
        assertFalse(ctx.definesLocation(tl));
        verifyTL(tl, false);

        when(tl.getName()).thenReturn("any");
        
        assertFalse(ctx.definesLocation(tl));
        verifyTL(tl, false);

        Tree t = when(mock(Tree.class).getName()).thenReturn(TOKENS_NODE_NAME).getMock();
        when(tl.getTree()).thenReturn(t);
        
        assertTrue(ctx.definesLocation(tl));
        verifyTL(tl, false);
        verify(t).getName();
        verifyNoMoreInteractions(t);
        reset(t);
        
        when(t.getName()).thenReturn("any").getMock();
        PropertyState primary = PropertyStates.createProperty(JCR_PRIMARYTYPE, TokenConstants.TOKEN_NT_NAME, Type.NAME);
        when(t.getProperty(JCR_PRIMARYTYPE)).thenReturn(primary);
        when(tl.getTree()).thenReturn(t);
        
        assertTrue(ctx.definesLocation(tl));
        verifyTL(tl, false);
        verify(t).getName();
        verify(t).getProperty(JCR_PRIMARYTYPE);
        verifyNoMoreInteractions(t);
        
        when(tl.getProperty()).thenReturn(ps);
        when(tl.getTree()).thenReturn(t);
        when(tl.getParent()).thenReturn(tl);

        assertTrue(ctx.definesLocation(tl));
        verifyTL(tl, true);

        PropertyState primary2 = PropertyStates.createProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.NAME);
        when(t.getProperty(JCR_PRIMARYTYPE)).thenReturn(primary2);

        when(tl.getProperty()).thenReturn(ps);
        when(tl.getTree()).thenReturn(t);
        when(tl.getParent()).thenReturn(tl);
        
        assertFalse(ctx.definesLocation(tl));
        verifyTL(tl, true);
        
        verifyNoInteractions(ps);
    }
    
    private static void verifyTL(@NotNull TreeLocation tl, boolean isProperty) {
        verify(tl).getProperty();
        verify(tl).getTree();
        if (isProperty) {
            verify(tl).getParent();
        }
        verifyNoMoreInteractions(tl);
        reset(tl);
    }
     
    @Test
    public void testDefinesInternal() {
        Tree t = when(mock(Tree.class).getName()).thenReturn(TOKENS_NODE_NAME).getMock();
        assertFalse(ctx.definesInternal(t));

        when(t.getName()).thenReturn("anyName");
        assertFalse(ctx.definesInternal(t));

        verifyNoInteractions(t);
    }
}