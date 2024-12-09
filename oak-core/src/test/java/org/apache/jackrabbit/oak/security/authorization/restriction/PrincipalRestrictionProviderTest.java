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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Value;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrincipalRestrictionProviderTest extends AbstractSecurityTest implements AccessControlConstants {

    private static final Logger log = LoggerFactory.getLogger(PrincipalRestrictionProviderTest.class);

    private RestrictionProvider base = mock(RestrictionProvider.class);
    private PrincipalRestrictionProvider provider = new PrincipalRestrictionProvider(base);

    @Test
    public void testGetSupportedDefinitions() {
        when(base.getSupportedRestrictions(anyString())).thenReturn(Collections.emptySet());

        Set<RestrictionDefinition> defs = provider.getSupportedRestrictions("/testPath");
        assertNotNull(defs);
        assertEquals(1, defs.size());
        assertEquals(REP_NODE_PATH, defs.iterator().next().getName());
    }

    @Test
    public void testCreateRestriction() throws Exception {
        Restriction r = mock(Restriction.class);
        Value v = mock(Value.class);
        when(base.createRestriction("/testPath", "name", v)).thenReturn(r);

        assertSame(r, provider.createRestriction("/testPath", "name", v));
        verify(base, times(1)).createRestriction("/testPath", "name", v);
    }

    @Test
    public void testCreateNodePathRestriction() throws Exception {
        Value emptyV = when(mock(Value.class).getString()).thenReturn("").getMock();
        Restriction r = provider.createRestriction("/testPath", REP_NODE_PATH, emptyV);
        assertTrue(r instanceof RestrictionImpl);
        assertTrue(r.getDefinition().isMandatory());
        assertEquals("", r.getProperty().getValue(Type.STRING));

        Value v = getValueFactory(root).createValue("/path");
        r = provider.createRestriction("/testPath", REP_NODE_PATH, v);
        assertTrue(r instanceof RestrictionImpl);
        assertEquals("/path", r.getProperty().getValue(Type.STRING));

        verify(base, never()).createRestriction(anyString(), anyString(), any(Value.class));
    }

    @Test
    public void testWriteRestrictions() throws Exception {
        Tree t = mock(Tree.class);
        Value v = getValueFactory(root).createValue("/path");
        PropertyState ps = PropertyStates.createProperty(REP_NODE_PATH, v);
        Set<Restriction> rs = Set.of(new RestrictionImpl(ps, true));
        provider.writeRestrictions("/testPath", t, rs);

        verify(base, never()).writeRestrictions("/testPath", t, new HashSet<>(rs));
        verify(base, times(1)).writeRestrictions("/testPath", t, new HashSet<>());
    }

    @Test
    public void testValidateRestrictions() throws Exception {
        Tree t = mock(Tree.class);
        provider.validateRestrictions("/testPath", t);

        verify(base, times(1)).validateRestrictions("/testPath", t);
    }
    
    @Test
    public void testGetPatternFromTree() throws Exception {
        Tree t = mock(Tree.class);
        when(base.getPattern("/testPath", t)).thenReturn(RestrictionPattern.EMPTY);

        assertSame(RestrictionPattern.EMPTY, provider.getPattern("/testPath", t));
        verify(base, times(1)).getPattern("/testPath", t);
    }

    @Test
    public void testGetPatternFromRestrictions() throws Exception {
        Restriction r = mock(Restriction.class);
        Set<Restriction> restrictions = Collections.singleton(r);
        provider.getPattern("/testPath", restrictions);

        verify(base, times(1)).getPattern("/testPath", restrictions);
    }
}