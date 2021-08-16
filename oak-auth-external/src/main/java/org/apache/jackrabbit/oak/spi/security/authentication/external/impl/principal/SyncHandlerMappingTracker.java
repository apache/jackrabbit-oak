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

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.jetbrains.annotations.NotNull;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_IDP_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME;

/**
 * {@code ServiceTracker} to detect any {@link SyncHandler} that has
 * dynamic membership enabled.
 */
final class SyncHandlerMappingTracker extends ServiceTracker {

    private static final Logger log = LoggerFactory.getLogger(SyncHandlerMappingTracker.class);

    private final Map<ServiceReference, Mapping> referenceMap = new HashMap<>();

    SyncHandlerMappingTracker(@NotNull BundleContext context) {
        super(context, SyncHandlerMapping.class.getName(), null);
    }

    @Override
    public Object addingService(ServiceReference reference) {
        addMapping(reference);
        return super.addingService(reference);
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
        addMapping(reference);
        super.modifiedService(reference, service);
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        referenceMap.remove(reference);
        super.removedService(reference, service);
    }

    private void addMapping(ServiceReference reference) {
        String idpName = PropertiesUtil.toString(reference.getProperty(PARAM_IDP_NAME), null);
        String syncHandlerName = PropertiesUtil.toString(reference.getProperty(PARAM_SYNC_HANDLER_NAME), null);

        if (idpName != null && syncHandlerName != null) {
            referenceMap.put(reference, new Mapping(syncHandlerName, idpName));
        } else {
            log.warn("Ignoring SyncHandlerMapping with incomplete mapping of IDP '{}' and SyncHandler '{}'", idpName, syncHandlerName);
        }
    }

    Iterable<String> getIdpNames(@NotNull final String syncHandlerName) {
        return Iterables.filter(Iterables.transform(referenceMap.values(), mapping -> {
            if (syncHandlerName.equals(mapping.syncHandlerName)) {
                return mapping.idpName;
            } else {
                // different synchandler name
                return null;
            }
        }), Predicates.notNull());
    }

    private static final class Mapping {
        private final String syncHandlerName;
        private final String idpName;

        private Mapping(@NotNull String syncHandlerName, @NotNull String idpName) {
            this.syncHandlerName = syncHandlerName;
            this.idpName = idpName;
        }
    }
}
