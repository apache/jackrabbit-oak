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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.SubtreeValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.Collections;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.DEFAULT_IDP_NAME;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class DynamicGroupValidatorProviderTest extends AbstractPrincipalTest {

    private final NodeState nsBefore = mock(NodeState.class);
    private final NodeState nsAfter = mock(NodeState.class);

    @Test
    public void testGetRootValidatorEmptyIdpNames() {
        DynamicGroupValidatorProvider provider = new DynamicGroupValidatorProvider(getRootProvider(), getTreeProvider(), getSecurityProvider(), Collections.emptySet());
        assertSame(DefaultValidator.INSTANCE, provider.getRootValidator(nsBefore, nsAfter, CommitInfo.EMPTY));
        verifyNoInteractions(nsBefore, nsAfter);
    }

    @Test
    public void testGetRootValidatorWithIdpName() {
        DynamicGroupValidatorProvider provider = new DynamicGroupValidatorProvider(getRootProvider(), getTreeProvider(), getSecurityProvider(), Collections.singleton(DEFAULT_IDP_NAME));
        Validator validator = provider.getRootValidator(nsBefore, nsAfter, CommitInfo.EMPTY);
        assertTrue(validator instanceof SubtreeValidator);
        verifyNoInteractions(nsBefore, nsAfter);
    }
}