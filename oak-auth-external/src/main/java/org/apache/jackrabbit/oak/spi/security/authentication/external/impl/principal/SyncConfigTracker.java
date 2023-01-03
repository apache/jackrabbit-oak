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

import com.google.common.collect.ObjectArrays;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipAware;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.jetbrains.annotations.NotNull;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * {@code ServiceTracker} to detect any {@link SyncHandler} that has
 * dynamic membership enabled.
 */
final class SyncConfigTracker extends ServiceTracker {

    private static final Logger log = LoggerFactory.getLogger(SyncConfigTracker.class);

    private final SyncHandlerMappingTracker mappingTracker;
    
    SyncConfigTracker(@NotNull BundleContext context, @NotNull SyncHandlerMappingTracker mappingTracker) {
        super(context, OsgiUtil.getFilter(SyncHandler.class, Collections.singletonMap(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, Boolean.TRUE.toString())), null);
        this.mappingTracker = mappingTracker;
    }

    /**
     * @return {@code true} if dynamic membership is enabled for at least one registered sync-handler; {@code false} otherwise.
     */
    boolean isEnabled() {
        return getReferences().length > 0;
    }

    /**
     * @return {@code true} if any of the register {@code SynchHandler} services has {@link DefaultSyncConfigImpl#PARAM_GROUP_DYNAMIC_GROUPS} 
     * enabled on top of the dynamic-membership.
     */
    boolean hasDynamicGroupsEnabled() {
        if (!isEnabled()) {
            return false;
        }
        for (ServiceReference ref : getReferences()) {
            if (PropertiesUtil.toBoolean(ref.getProperty(DefaultSyncConfigImpl.PARAM_GROUP_DYNAMIC_GROUPS), DefaultSyncConfigImpl.PARAM_GROUP_DYNAMIC_GROUPS_DEFAULT)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the set of IDP names from the {@code org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping} 
     * for which the associated {@code SyncHandler} is configured to have both {@link DefaultSyncConfigImpl#PARAM_USER_DYNAMIC_MEMBERSHIP dynamic membership} 
     * and {@link DefaultSyncConfigImpl#PARAM_GROUP_DYNAMIC_GROUPS dynamic groups} enabled.
     * 
     * @return a set of IDP names that have dynamic groups enabled in addition to dynamic membership. If {@link #hasDynamicGroupsEnabled()}
     * returns false, this method will return an empty set.
     * @see #hasDynamicGroupsEnabled() 
     */
    @NotNull
    Set<String> getIdpNamesWithDynamicGroups() {
        if (!isEnabled()) {
            return Collections.emptySet();
        }

        ServiceReference[] serviceReferences = getServiceReferences();
        Set<String> idpNames = new HashSet<>(serviceReferences.length);
        for (ServiceReference ref : serviceReferences) {
            if (PropertiesUtil.toBoolean(ref.getProperty(DefaultSyncConfigImpl.PARAM_GROUP_DYNAMIC_GROUPS), DefaultSyncConfigImpl.PARAM_GROUP_DYNAMIC_GROUPS_DEFAULT)) {
                String syncHandlerName = PropertiesUtil.toString(ref.getProperty(DefaultSyncConfigImpl.PARAM_NAME), DefaultSyncConfigImpl.PARAM_NAME_DEFAULT);
                for (String idpName : mappingTracker.getIdpNames(syncHandlerName)) {
                    idpNames.add(idpName);
                }
            }
        }
        return idpNames;
    }

    @NotNull
    Map<String, String[]> getAutoMembership() {
        Map<String, String[]> autoMembership = new HashMap<>();
        for (ServiceReference ref : getReferences()) {
            String syncHandlerName = PropertiesUtil.toString(ref.getProperty(DefaultSyncConfigImpl.PARAM_NAME), DefaultSyncConfigImpl.PARAM_NAME_DEFAULT);
            String[] userAuthMembership = PropertiesUtil.toStringArray(ref.getProperty(DefaultSyncConfigImpl.PARAM_USER_AUTO_MEMBERSHIP), new String[0]);
            String[] groupAuthMembership = PropertiesUtil.toStringArray(ref.getProperty(DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP), new String[0]);

            populateMap(syncHandlerName, ObjectArrays.concat(userAuthMembership, groupAuthMembership, String.class), autoMembership);
        }
        return autoMembership;
    }

    @NotNull
    Map<String, String[]> getGroupAutoMembership() {
        Map<String, String[]> autoMembership = new HashMap<>();
        for (ServiceReference ref : getReferences()) {
            String syncHandlerName = PropertiesUtil.toString(ref.getProperty(DefaultSyncConfigImpl.PARAM_NAME), DefaultSyncConfigImpl.PARAM_NAME_DEFAULT);
            String[] groupAuthMembership = PropertiesUtil.toStringArray(ref.getProperty(DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP), new String[0]);
            populateMap(syncHandlerName, groupAuthMembership, autoMembership);
        }
        return autoMembership;
    }

    private void populateMap(@NotNull String syncHandlerName, @NotNull String[] autoMembershipParam, @NotNull Map<String, String[]> autoMembership) {
        for (String idpName : mappingTracker.getIdpNames(syncHandlerName)) {
            String[] previous = autoMembership.put(idpName, autoMembershipParam);
            if (previous != null) {
                String msg = (Arrays.equals(previous, autoMembershipParam)) ? "Duplicate" : "Colliding";
                String prev = Arrays.toString(previous);
                String mbrs = Arrays.toString(autoMembershipParam);
                log.debug("{} group auto-membership configuration for IDP '{}'; replacing previous values {} by {} defined by SyncHandler '{}'",
                        msg, idpName, prev, mbrs, syncHandlerName);
            }
        }
    }
    
    @NotNull 
    Map<String, AutoMembershipConfig> getAutoMembershipConfig() {
        Map<String, AutoMembershipConfig> amMap = new HashMap<>();
        for (ServiceReference ref : getReferences()) {
            String syncHandlerName = PropertiesUtil.toString(ref.getProperty(DefaultSyncConfigImpl.PARAM_NAME), DefaultSyncConfigImpl.PARAM_NAME_DEFAULT);
            Object shService = getService(ref);
            if (shService instanceof AutoMembershipAware) {
                AutoMembershipConfig config = ((AutoMembershipAware) shService).getAutoMembershipConfig();
                for (String idpName : mappingTracker.getIdpNames(syncHandlerName)) {
                    AutoMembershipConfig previous = amMap.put(idpName, config);
                    if (previous != null) {
                        String msg = (previous.equals(config)) ? "Duplicate" : "Colliding";
                        log.debug("{} auto-membership configuration for IDP '{}'; replacing previous values {} by {} defined by SyncHandler '{}'",
                                msg, idpName, previous, config, syncHandlerName);
                    }
                }            
            }
        }
        return amMap;
    }
    
    @NotNull
    private ServiceReference[] getReferences() {
        ServiceReference[] refs = getServiceReferences();
        return (refs == null) ? new ServiceReference[0] : refs;
    }
}
