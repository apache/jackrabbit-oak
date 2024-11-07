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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthorizableIteratorTest extends AbstractSecurityTest {

    private Iterator<Tree> userTreeIterator;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        userTreeIterator = Iterators.singletonIterator(root.getTree(getNamePathMapper().getOakPath(getTestUser().getPath())));
    }

    @Test
    public void testGetSize() {
        AuthorizableIterator it = AuthorizableIterator.create(userTreeIterator, (UserManagerImpl) getUserManager(root), AuthorizableType.USER);
        assertEquals(-1, it.getSize());
    }

    @Test
    public void testGetSize2() {
        AuthorizableIterator it = AuthorizableIterator.create(RangeIteratorAdapter.EMPTY, (UserManagerImpl) getUserManager(root), AuthorizableType.USER);
        assertEquals(RangeIteratorAdapter.EMPTY.getSize(), it.getSize());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        AuthorizableIterator it = AuthorizableIterator.create(userTreeIterator, (UserManagerImpl) getUserManager(root), AuthorizableType.USER);
        it.next();
        it.remove();
    }

    @Test
    public void testTypeMatch() {
        AuthorizableIterator it = AuthorizableIterator.create(userTreeIterator, (UserManagerImpl) getUserManager(root), AuthorizableType.USER);
        assertTrue(it.hasNext());
        assertTrue(it.next() instanceof User);
    }

    @Test
    public void testTypeMatch2() {
        AuthorizableIterator it = AuthorizableIterator.create(userTreeIterator, (UserManagerImpl) getUserManager(root), AuthorizableType.AUTHORIZABLE);
        assertTrue(it.hasNext());
        assertTrue(it.next() instanceof User);
    }

    @Test
    public void testTypeMismatch() {
        AuthorizableIterator it = AuthorizableIterator.create(userTreeIterator, (UserManagerImpl) getUserManager(root), AuthorizableType.GROUP);
        assertFalse(it.hasNext());
    }

    @Test
    public void testInvalidPath() {
        AuthorizableIterator it = AuthorizableIterator.create(Iterators.singletonIterator(root.getTree(PathUtils.ROOT_PATH)), (UserManagerImpl) getUserManager(root), AuthorizableType.AUTHORIZABLE);
        assertFalse(it.hasNext());
    }
    
    @Test
    public void testFilterDuplicates() throws Exception {
        List<Authorizable> l = ImmutableList.of(getTestUser());
        assertEquals(1, Iterators.size(AuthorizableIterator.create(true, l.iterator(), l.iterator())));
        assertEquals(2, Iterators.size(AuthorizableIterator.create(false, l.iterator(), l.iterator())));
        
        // duplications are determined base on authorizableID
        Authorizable a = when(mock(Authorizable.class).getID()).thenReturn(getTestUser().getID()).getMock();
        assertEquals(1, Iterators.size(AuthorizableIterator.create(true, l.iterator(), Iterators.singletonIterator(a))));
    }

    @Test
    public void testFilterDuplicatesHandlesNull() throws Exception {
        List<User> l = Arrays.asList(getTestUser(), null, getTestUser());
        assertEquals(1, Iterators.size(AuthorizableIterator.create(true, l.iterator(), l.iterator())));
    }

    @Test
    public void testFilterDuplicatesGetIdFails() throws Exception {
        Authorizable a = when(mock(Authorizable.class).getID()).thenThrow(new RepositoryException()).getMock();

        List<Authorizable> l = ImmutableList.of(getTestUser(), a);
        assertEquals(1, Iterators.size(AuthorizableIterator.create(true, l.iterator(), Collections.emptyIterator())));
    }

    @Test
    public void testGetSize3() throws Exception {
        List<User> l = List.of(getTestUser());

        // size cannot be computed from regular iterators
        assertEquals(-1, AuthorizableIterator.create(false, l.iterator(), l.iterator()).getSize());
        assertEquals(-1, AuthorizableIterator.create(true, l.iterator(), l.iterator()).getSize());

        // size can be computed from regular iterators but filters are only apply upon iteration
        RangeIteratorAdapter adapter = new RangeIteratorAdapter(l);
        assertEquals(2, AuthorizableIterator.create(false, adapter, adapter).getSize());
        assertEquals(2, AuthorizableIterator.create(true, adapter, adapter).getSize());
    }

    @Test
    public void testGetSize4() {
        assertEquals(0, AuthorizableIterator.create(Collections.emptyIterator(), (UserManagerImpl) getUserManager(root), AuthorizableType.AUTHORIZABLE).getSize());
        assertEquals(0, AuthorizableIterator.create(true, Collections.emptyIterator(), Collections.emptyIterator()).getSize());
    }
}