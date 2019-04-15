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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilterProviderImplTest {

    private static final String PATH = "/supported/path";

    private FilterProviderImpl provider = AbstractPrincipalBasedTest.createFilterProviderImpl(PATH);

    @Test
    public void testHandlesPath() {
        assertFalse(provider.handlesPath(PathUtils.ROOT_PATH));
        assertFalse(provider.handlesPath(PATH + "sibling"));
        assertTrue(provider.handlesPath(PATH));
        assertTrue(provider.handlesPath(PathUtils.concat(PATH, "a", "path", "below")));
    }

    @Test
    public void testGetSearchRoot() {
        assertEquals(PATH, provider.getFilterRoot());
    }

    @Test
    public void testGetFilter() {
        SecurityProvider sp = Mockito.spy(SecurityProviderBuilder.newBuilder().build());
        Root root = mock(Root.class);

        Filter filter = provider.getFilter(sp, root, NamePathMapper.DEFAULT);
        assertNotNull(filter);

        Mockito.verify(sp, Mockito.times(1)).getConfiguration(PrincipalConfiguration.class);
    }

    @Test
    public void testActivate() {
        FilterProviderImpl fp = new FilterProviderImpl();
        assertNull(fp.getFilterRoot());

        fp.activate(when(mock(FilterProviderImpl.Configuration.class).path()).thenReturn(PATH).getMock(), Collections.emptyMap());
        assertEquals(PATH, fp.getFilterRoot());
    }

    @Test
    public void testModified() {
        String modifiedPath = "/modified/path";
        provider.modified(when(mock(FilterProviderImpl.Configuration.class).path()).thenReturn(modifiedPath).getMock(), Collections.emptyMap());
        assertEquals(modifiedPath, provider.getFilterRoot());
    }
}