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
package org.apache.jackrabbit.oak.spi.security.principal;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.junit.Test;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CompositePrincipalProviderTest extends AbstractSecurityTest{

    @Test
    public void testOfEmptyList() {
        assertSame(EmptyPrincipalProvider.INSTANCE, CompositePrincipalProvider.of(ImmutableList.<PrincipalProvider>of()));
    }

    @Test
    public void testOfSingletonList() {
        PrincipalProvider pp = getSecurityProvider().getConfiguration(PrincipalConfiguration.class).getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertSame(pp, CompositePrincipalProvider.of(ImmutableList.of(pp)));
    }

    @Test
    public void testOfList() {
        PrincipalProvider pp = getSecurityProvider().getConfiguration(PrincipalConfiguration.class).getPrincipalProvider(root, NamePathMapper.DEFAULT);

        PrincipalProvider of = CompositePrincipalProvider.of(ImmutableList.of(pp, pp));
        assertNotSame(pp, of);
        assertTrue(of instanceof CompositePrincipalProvider);
    }
}