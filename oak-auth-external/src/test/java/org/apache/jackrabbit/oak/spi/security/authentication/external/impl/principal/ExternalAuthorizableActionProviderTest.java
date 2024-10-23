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

import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.GroupAction;
import org.apache.sling.testing.mock.osgi.MapUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class ExternalAuthorizableActionProviderTest extends AbstractExternalAuthTest {
    
    private static final String IDP_NAME = "idp1";
    private static final String LOCAL_GROUP_ID = "localGroup";

    private final ExternalAuthorizableActionProvider eap = new ExternalAuthorizableActionProvider();
    
    private final Group localGroup = mock(Group.class);
    private final Group externalGroup = mock(Group.class);
    private final Authorizable externalUser = mock(Authorizable.class);
    
    private SyncHandler sh;
    private SyncHandlerMapping shMapping;

    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { true, "Fail" },
                new Object[] { false, "Only Warn" });
    }
    
    private final boolean failOnViolation;
    
    public ExternalAuthorizableActionProviderTest(boolean failOnViolation, String name) {
        this.failOnViolation = failOnViolation;
    }
    
    @Before
    public void before() throws Exception {
        super.before();
        
        eap.activate(context.bundleContext(), new ExternalAuthorizableActionProvider.Configuration() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return Annotation.class;
            }

            @Override
            public boolean failAddMembersForDifferentIdp() {
                return failOnViolation;
            }
        });
        
        doReturn(LOCAL_GROUP_ID).when(localGroup).getID();
        doReturn("externalGroup").when(externalGroup).getID();
        
        Value v = getValueFactory(root).createValue(new ExternalIdentityRef("externalUserId", IDP_NAME).getString());
        when(externalUser.getProperty(REP_EXTERNAL_ID)).thenReturn(new Value[] {v});
        when(externalUser.isGroup()).thenReturn(false);

        v = getValueFactory(root).createValue(new ExternalIdentityRef("externalGroupId", IDP_NAME).getString());
        when(externalGroup.getProperty(REP_EXTERNAL_ID)).thenReturn(new Value[] {v});
        when(externalGroup.isGroup()).thenReturn(true);
        
        sh = new DefaultSyncHandler();
        shMapping = new SyncHandlerMapping() {};
    }
    
    @After
    public void after() throws Exception {
        try {
            reset(localGroup, externalGroup, externalUser);
        } finally {
            super.after();
        }
    }
    
    private GroupAction getGroupAction() {
        return (GroupAction) eap.getAuthorizableActions(getSecurityProvider()).get(0);
    }

    @Test
    public void testGetActions() {
        List<? extends AuthorizableAction> actions = eap.getAuthorizableActions(getSecurityProvider());
        assertEquals(1, actions.size());
        assertTrue(actions.get(0) instanceof GroupAction);
    }
    
    @Test
    public void testMemberNotExternal() throws Exception {
        GroupAction ga = getGroupAction();
        
        ga.onMemberAdded(localGroup, getTestUser(), root, getNamePathMapper());
        ga.onMemberAdded(externalGroup, getTestUser(), root, getNamePathMapper());
        
        verifyNoInteractions(localGroup);
        verifyNoInteractions(externalGroup);
    }

    @Test
    public void testSameIDP() throws Exception {
        GroupAction ga = getGroupAction();
        ga.onMemberAdded(externalGroup, externalUser, root, getNamePathMapper());

        verify(externalGroup).getProperty(REP_EXTERNAL_ID);
        verifyNoMoreInteractions(externalGroup);
    }
    
    @Test
    public void testUserMamaberGroupInUserAutomembership() throws Exception {
        registerSyncHandlerSyncMapping(IDP_NAME, LOCAL_GROUP_ID, true);

        GroupAction ga = getGroupAction();
        ga.onMemberAdded(localGroup, externalUser, root, getNamePathMapper());
        
        verify(localGroup).getProperty(REP_EXTERNAL_ID);
        verify(localGroup).getID();
        verifyNoMoreInteractions(localGroup);

        ga.onMemberAdded(localGroup, externalUser, root, getNamePathMapper());
        verify(localGroup, times(2)).getProperty(REP_EXTERNAL_ID);
        verify(localGroup, times(2)).getID();
        verifyNoMoreInteractions(localGroup);
    }

    @Test
    public void testUserMemberGroupInGroupAutomembership() throws Exception {
        registerSyncHandlerSyncMapping(IDP_NAME, LOCAL_GROUP_ID, false);
        assertViolationDetected(externalUser);
        verifyInvocations();
    }

    @Test
    public void testGroupMemberGroupInAutomembership() throws Exception {
        registerSyncHandlerSyncMapping(IDP_NAME, LOCAL_GROUP_ID, false);

        GroupAction ga = getGroupAction();
        ga.onMemberAdded(localGroup, externalGroup, root, getNamePathMapper());

        verify(localGroup).getProperty(REP_EXTERNAL_ID);
        verify(localGroup).getID();
        verifyNoMoreInteractions(localGroup);

        ga.onMemberAdded(localGroup, externalGroup, root, getNamePathMapper());
        verify(localGroup, times(2)).getProperty(REP_EXTERNAL_ID);
        verify(localGroup, times(2)).getID();
        verifyNoMoreInteractions(localGroup);
    }

    @Test
    public void testGroupMemberGroupInUserAutomembership() throws Exception {
        registerSyncHandlerSyncMapping(IDP_NAME, LOCAL_GROUP_ID, true);
        assertViolationDetected(externalGroup);
        verifyInvocations();
    }

    @Test
    public void testGroupInAutomembershipDifferentIDP() throws Exception {
        registerSyncHandlerSyncMapping("anotherIDP", LOCAL_GROUP_ID, true);
        assertViolationDetected(externalUser);
        verifyInvocations();
    }

    @Test
    public void testGroupNotInAutomembership() throws Exception {
        registerSyncHandlerSyncMapping(IDP_NAME, "anotherGroup", false);
        assertViolationDetected(externalGroup);
        verifyInvocations();
    }

    @Test
    public void testModifySyncHandler() throws Exception {
        Map<String,Object> config = createSyncConfig(LOCAL_GROUP_ID, true);
        ServiceRegistration sr = context.bundleContext().registerService(SyncHandler.class.getName(), sh, MapUtil.toDictionary(config));
        context.registerService(SyncHandlerMapping.class, shMapping, MapUtil.toDictionary(createMappingConfig(IDP_NAME)));

        getGroupAction().onMemberAdded(localGroup, externalUser, root, getNamePathMapper());

        // modify sync-handler
        sr.setProperties(MapUtil.toDictionary(createSyncConfig("anotherGroup", true)));
        // FIXME: use osgi-mock to invoke the method
        Field f = ExternalAuthorizableActionProvider.class.getDeclaredField("automembershipTracker");
        f.setAccessible(true);
        Object automembershipTracker = f.get(eap);
        Method m = automembershipTracker.getClass().getDeclaredMethod("modifiedService", ServiceReference.class, Object.class);
        m.setAccessible(true);
        m.invoke(automembershipTracker, sr.getReference(), sh);
        // now onMemberAdd will spot the violation
        assertViolationDetected(externalUser);
    }
    
    @Test
    public void testRemoveSyncHandler() throws RepositoryException {
        Map<String,Object> config = createSyncConfig(LOCAL_GROUP_ID, false);
        ServiceRegistration sr = context.bundleContext().registerService(SyncHandler.class.getName(), sh, MapUtil.toDictionary(config));
        context.registerService(SyncHandlerMapping.class, shMapping, MapUtil.toDictionary(createMappingConfig(IDP_NAME)));
        
        getGroupAction().onMemberAdded(localGroup, externalGroup, root, getNamePathMapper());
        
        // remove sync-handler
        sr.unregister();
        // now onMemberAdd will spot the violation
        assertViolationDetected(externalGroup);
    }
    
    @Test
    public void testDuplicateSyncHandlerMapping() throws Exception {
        registerSyncHandlerSyncMapping("anotherIdpName", LOCAL_GROUP_ID, true);
        // register another sync-handler-mapping with the same properties
        context.registerService(SyncHandlerMapping.class, new SyncHandlerMapping() {}, createMappingConfig("anotherIdpName"));
        assertViolationDetected(externalUser);
    }

    @Test
    public void testCollidingSyncHandler() throws Exception {
        registerSyncHandlerSyncMapping("anotherIdpName", LOCAL_GROUP_ID, true);
        // register another sync-handler with the same properties
        SyncHandler anotherSh = new DefaultSyncHandler();
        context.registerInjectActivateService(anotherSh, createSyncConfig("anotherGroupId", true));

        assertViolationDetected(externalUser);
    }
    
    @Test
    public void testDeactivate() throws RepositoryException {
        // onMemberAdd will spot the violation
        assertViolationDetected(externalUser);
        
        // after deactivation, changes to sync-handler registration will no longer be tracked
        eap.deactivate();
        registerSyncHandlerSyncMapping(IDP_NAME, "localGroup", true);

        // -> violation still detected
        assertViolationDetected(externalUser);
    }

    @Test
    public void testDeactivatePriorToActivate() throws RepositoryException {
        ExternalAuthorizableActionProvider provider = new ExternalAuthorizableActionProvider();
        provider.deactivate();
        
        registerSyncHandlerSyncMapping(IDP_NAME, "localGroup", true);
        // -> violation still detected
        GroupAction groupAction = (GroupAction) provider.getAuthorizableActions(getSecurityProvider()).get(0);
        // note: without prior activation 'failOnViolation' is not initialized and thus 'false'
        assertViolationDetected(externalUser, groupAction, false);
    }

    private void assertViolationDetected(@NotNull Authorizable externalMember) throws RepositoryException {
        assertViolationDetected(externalMember, getGroupAction(), failOnViolation);
    }

    private void assertViolationDetected(@NotNull Authorizable externalMember, @NotNull GroupAction ga, boolean failOnViolation) throws RepositoryException {
        try {
            ga.onMemberAdded(localGroup, externalMember, root, getNamePathMapper());
            if (failOnViolation) {
                fail("ConstraintViolationException expected");
            }
        } catch (ConstraintViolationException e) {
            if (!failOnViolation) {
                fail("No ConstraintViolationException expected");
            }
        }
    }
    
    private void verifyInvocations() throws RepositoryException {
        verify(localGroup).getProperty(REP_EXTERNAL_ID);
        verify(localGroup).getID();
        verifyNoMoreInteractions(localGroup);
    }
    
    private void registerSyncHandlerSyncMapping(@NotNull String idpName, @NotNull String automembershipId, boolean isUserAutoMembership) {
        context.registerInjectActivateService(sh, createSyncConfig(automembershipId, isUserAutoMembership));
        context.registerService(SyncHandlerMapping.class, shMapping, createMappingConfig(idpName));
    }
    
    private static Map<String, Object> createSyncConfig(@NotNull String automembershipId, boolean isUserAutoMembership) {
        String autoMembershipName = (isUserAutoMembership) ? DefaultSyncConfigImpl.PARAM_USER_AUTO_MEMBERSHIP : DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP;
        return ImmutableMap.of(
                DefaultSyncConfigImpl.PARAM_NAME, "sh",
                autoMembershipName, new String[] {automembershipId});
    }

    private static Map<String, Object> createMappingConfig(@NotNull String idpName) {
        return ImmutableMap.of(
                SyncHandlerMapping.PARAM_IDP_NAME, idpName,
                SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME, "sh");
    }
}