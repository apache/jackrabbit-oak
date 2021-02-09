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
package org.apache.jackrabbit.oak.spi.security.authorization.restriction;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class EmptyRestrictionProviderTest {

    @Test
    public void testGetSupportedRestrictions() {
        assertTrue(RestrictionProvider.EMPTY.getSupportedRestrictions(null).isEmpty());
        assertTrue(RestrictionProvider.EMPTY.getSupportedRestrictions("/any/path").isEmpty());
    }

    @Test(expected = AccessControlException.class)
    public void testCreateRestrictionSingleValue() throws RepositoryException {
        Value v = mock(Value.class);
        RestrictionProvider.EMPTY.createRestriction(null, "name", v);
    }

    @Test(expected = AccessControlException.class)
    public void testCreateRestrictionMvValues() throws RepositoryException {
        Value v = mock(Value.class);
        RestrictionProvider.EMPTY.createRestriction(null, "name", v, v);
    }

    @Test(expected = AccessControlException.class)
    public void testCreateRestrictionEmptyValues() throws RepositoryException {
        RestrictionProvider.EMPTY.createRestriction(null, "name");
    }

    @Test
    public void testReadRestrictions() {
        assertTrue(RestrictionProvider.EMPTY.readRestrictions(null, mock(Tree.class)).isEmpty());
        assertTrue(RestrictionProvider.EMPTY.readRestrictions("/any/path", mock(Tree.class)).isEmpty());
    }

    @Test
    public void testWriteRestrictions() throws Exception {
        Restriction r = mock(Restriction.class);
        RestrictionProvider.EMPTY.writeRestrictions(null, mock(Tree.class), ImmutableSet.of(r));
        RestrictionProvider.EMPTY.writeRestrictions("/any/path", mock(Tree.class), ImmutableSet.of(r));
        verifyNoInteractions(r);
    }

    @Test
    public void testValidateRestrictions() throws Exception {
        Tree t = mock(Tree.class);
        RestrictionProvider.EMPTY.validateRestrictions(null, t);
        RestrictionProvider.EMPTY.validateRestrictions("/any/path", t);
        verifyNoInteractions(t);
    }

    @Test
    public void testGetPattern() {
        Restriction r = mock(Restriction.class);
        assertSame(RestrictionPattern.EMPTY, RestrictionProvider.EMPTY.getPattern(null, ImmutableSet.of(r)));
        assertSame(RestrictionPattern.EMPTY, RestrictionProvider.EMPTY.getPattern("/any/path", ImmutableSet.of(r)));
    }

    @Test
    public void testGetPatternFromTree() {
        assertSame(RestrictionPattern.EMPTY, RestrictionProvider.EMPTY.getPattern(null, mock(Tree.class)));
        assertSame(RestrictionPattern.EMPTY, RestrictionProvider.EMPTY.getPattern("/any/path", mock(Tree.class)));
    }
}