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

import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.service.component.ComponentContext;

/**
 * {@code SyncManagerImpl} is used to manage registered sync handlers. This class automatically
 * tracks the SyncHandlers that are registered via OSGi but can also be used in non-OSGi environments by manually
 * adding and removing the handlers.
 */
@Component(immediate = true)
@Service
public class SyncManagerImpl extends AbstractServiceTracker<SyncHandler> implements SyncManager {

    /**
     * Default constructor used by OSGi
     */
    @SuppressWarnings("UnusedDeclaration")
    public SyncManagerImpl() {
        super(SyncHandler.class);
    }

    /**
     * Constructor used by non OSGi
     * @param whiteboard the whiteboard
     */
    public SyncManagerImpl(Whiteboard whiteboard) {
        super(SyncHandler.class);
        start(whiteboard);
    }

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(ComponentContext ctx) {
        start(new OsgiWhiteboard(ctx.getBundleContext()));
    }

    @SuppressWarnings("UnusedDeclaration")
    @Deactivate
    private void deactivate() {
        stop();
    }

    @Override
    public SyncHandler getSyncHandler(@Nonnull String name) {
        for (SyncHandler handler: getServices()) {
            if (name.equals(handler.getName())) {
                return handler;
            }
        }
        return null;
    }
}