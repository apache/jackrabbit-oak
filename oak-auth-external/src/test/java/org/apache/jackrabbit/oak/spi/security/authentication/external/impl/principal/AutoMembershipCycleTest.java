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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.ValueFactory;
import java.util.Collections;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutoMembershipCycleTest extends AbstractAutoMembershipTest {

    private AutoMembershipPrincipals amp;
    private UserManager umMock;
    private Group gr;
    private Group gr2;
    private Group amGr1;
    private final Authorizable authorizable = mock(Authorizable.class);
    
    @Before
    public void before() throws Exception {
        super.before();
        Map<String, String[]> mapping = Collections.singletonMap(IDP_VALID_AM, new String[] {AUTOMEMBERSHIP_GROUP_ID_1});
        
        umMock = spy(userManager);
        amGr1 = spy(automembershipGroup1);
        gr = spy(umMock.createGroup("testGroup"));
        gr2 = spy(umMock.createGroup("testGroup2"));
        when(umMock.getAuthorizable(AUTOMEMBERSHIP_GROUP_ID_1)).thenReturn(amGr1);
        
        umMock.createGroup(EveryonePrincipal.getInstance());
        root.commit();
        
        
        amp = new AutoMembershipPrincipals(umMock, mapping, getAutoMembershipConfigMapping());
        clearInvocations(umMock, amGr1, gr, gr2);
    }

    @Override
    public void after() throws Exception {
        try {
            amGr1.removeMembers("testGroup");
            gr.remove();
            gr2.remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(UserConfiguration.NAME,
                ConfigurationParameters.of(
                        ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT)
        );
    }

    @Override
    protected @NotNull DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig dsc = super.createSyncConfig();
        dsc.user().setDynamicMembership(true).setAutoMembership(AUTOMEMBERSHIP_GROUP_ID_1);
        dsc.group().setDynamicGroups(true).setAutoMembership(AUTOMEMBERSHIP_GROUP_ID_1);
        return dsc;
    }

    @Test
    public void testIsInheritedMemberCircularIncludingAutoMembership() throws Exception {
        // create cycle
        assertTrue(amGr1.addMembers(gr.getID()).isEmpty());
        assertTrue(gr.addMembers(amGr1.getID()).isEmpty());
        root.commit();
        clearInvocations(gr, amGr1);

        // declared automembership
        assertTrue(amp.isInheritedMember(IDP_VALID_AM, amGr1, authorizable));
        verify(amGr1).getID();
        verifyNoMoreInteractions(amGr1);
        verifyNoInteractions(authorizable, gr);
        clearInvocations(authorizable, gr, amGr1);

        // declared automembership in amGr1 (cycle)
        assertTrue(amp.isInheritedMember(IDP_VALID_AM, gr, authorizable));
        verifyNoInteractions(authorizable);
        verify(gr).getID();
        verify(amGr1).declaredMemberOf();
        verify(umMock).getAuthorizable(AUTOMEMBERSHIP_GROUP_ID_1);
        verifyNoMoreInteractions(gr);
        clearInvocations(authorizable, amGr1, gr, umMock);

        // declared automembership for group
        assertTrue(amp.isInheritedMember(IDP_VALID_AM, amGr1, gr));
        verify(amGr1).getID();
        verifyNoMoreInteractions(amGr1);
        verifyNoInteractions(gr, umMock);
        clearInvocations(amGr1, gr, umMock);

        // declared automembership in amGr1 (cycle)
        assertTrue(amp.isInheritedMember(IDP_VALID_AM, gr, amGr1));
        verify(gr).getID();
        verify(amGr1).declaredMemberOf();
        verify(umMock).getAuthorizable(AUTOMEMBERSHIP_GROUP_ID_1);

        verifyNoMoreInteractions(gr, umMock);
    }

    @Test
    public void testIsInheritedMemberCircularWithoutAutoMembership() throws Exception {
        // create cycle
        assertTrue(gr.addMembers(gr2.getID()).isEmpty());
        assertTrue(gr2.addMembers(gr.getID()).isEmpty());
        root.commit();
        clearInvocations(gr, gr2);
        
        assertFalse(amp.isInheritedMember(IDP_VALID_AM, gr, gr2));
        
        verify(gr, times(1)).getID();
        verifyNoMoreInteractions(gr, gr2);
    }
    
    @Test
    public void testIsInheritedMemberCircularIncludingExternalGroup() throws Exception {
        externalPrincipalConfiguration.setParameters(ConfigurationParameters.of(
                PARAM_PROTECT_EXTERNAL_IDS, false));

        // create cycle
        assertTrue(amGr1.addMembers(gr.getID()).isEmpty());
        assertTrue(gr.addMembers(amGr1.getID()).isEmpty());
        root.commit();

        // mark groups as external identities
        ValueFactory vf = getValueFactory(root);
        gr.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, vf.createValue(gr.getID() + ';' + idp.getName()));
        amGr1.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, vf.createValue(amGr1.getID() + ';' + idp.getName()));
        root.commit();
        clearInvocations(gr, amGr1);

        assertTrue(amp.isInheritedMember(IDP_VALID_AM, gr, authorizable));
        assertTrue(amp.isInheritedMember(IDP_VALID_AM, gr, amGr1));
    }
}