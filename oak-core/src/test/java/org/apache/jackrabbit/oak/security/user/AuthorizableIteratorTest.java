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

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuthorizableIteratorTest extends AbstractSecurityTest {

    private Iterator<String> userOakPathIterator;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        userOakPathIterator = Iterators.singletonIterator(getNamePathMapper().getOakPath(getTestUser().getPath()));
    }

    @Test
    public void testGetSize() {
        AuthorizableIterator it = AuthorizableIterator.create(userOakPathIterator, (UserManagerImpl) getUserManager(root), AuthorizableType.USER);
        assertEquals(-1, it.getSize());
    }

    @Test
    public void testGetSize2() {
        AuthorizableIterator it = AuthorizableIterator.create(RangeIteratorAdapter.EMPTY, (UserManagerImpl) getUserManager(root), AuthorizableType.USER);
        assertEquals(RangeIteratorAdapter.EMPTY.getSize(), it.getSize());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        AuthorizableIterator it = AuthorizableIterator.create(userOakPathIterator, (UserManagerImpl) getUserManager(root), AuthorizableType.USER);
        it.next();
        it.remove();
    }

    @Test
    public void testTypeMatch() {
        AuthorizableIterator it = AuthorizableIterator.create(userOakPathIterator, (UserManagerImpl) getUserManager(root), AuthorizableType.USER);
        assertTrue(it.hasNext());
        assertTrue(it.next() instanceof User);
    }

    @Test
    public void testTypeMatch2() {
        AuthorizableIterator it = AuthorizableIterator.create(userOakPathIterator, (UserManagerImpl) getUserManager(root), AuthorizableType.AUTHORIZABLE);
        assertTrue(it.hasNext());
        assertTrue(it.next() instanceof User);
    }

    @Test
    public void testTypeMismatch() {
        AuthorizableIterator it = AuthorizableIterator.create(userOakPathIterator, (UserManagerImpl) getUserManager(root), AuthorizableType.GROUP);
        assertFalse(it.hasNext());
    }

    @Test
    public void testInvalidPath() {
        AuthorizableIterator it = AuthorizableIterator.create(Iterators.singletonIterator(PathUtils.ROOT_PATH), (UserManagerImpl) getUserManager(root), AuthorizableType.AUTHORIZABLE);
        assertFalse(it.hasNext());
    }
}