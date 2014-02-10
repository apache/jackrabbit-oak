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

package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;

/**
 * {@code SyncManagerImpl} is used to manage registered sync handlers. This class automatically
 * tracks the SyncHandlers that are registered via OSGi but can also be used in non-OSGi environments by manually
 * adding and removing the handlers.
 */
@Component
@Service
public class SyncManagerImpl implements SyncManager {

    @Reference(
            name = "syncHandler",
            bind = "addHandler",
            unbind = "removeHandler",
            referenceInterface = SyncHandler.class,
            cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
    final private Map<String, SyncHandler> handlers = new ConcurrentHashMap<String, SyncHandler>();

    public void addHandler(SyncHandler handler, final Map<String, Object> props) {
        handlers.put(handler.getName(), handler);
    }

    public void removeHandler(SyncHandler handler, final Map<String, Object> props) {
        handlers.remove(handler.getName());
    }

    @Override
    public SyncHandler getSyncHandler(@Nonnull String name) {
        return handlers.get(name);
    }
}