/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.osgi;

import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.framework.ServiceReference;

/**
 * Implements a service tracker that keeps track of all
 * {@link RepositoryInitializer}s in the system and calls the available
 * method once the micro kernel is available.
 */
public class OsgiRepositoryInitializer
        extends AbstractServiceTracker<RepositoryInitializer>
        implements RepositoryInitializer {

    private RepositoryInitializerObserver observer;

    public OsgiRepositoryInitializer() {
        super(RepositoryInitializer.class);
    }

    @Override
    public NodeState initialize(NodeState state) {
        return new CompositeInitializer(getServices()).initialize(state);
    }

    @Override
    public Object addingService(ServiceReference reference) {
        RepositoryInitializer ri = (RepositoryInitializer) super
                .addingService(reference);
        if (observer != null) {
            observer.newRepositoryInitializer(ri);
        }
        return ri;
    }

    public void setObserver(RepositoryInitializerObserver observer) {
        this.observer = observer;
    }

    interface RepositoryInitializerObserver {
        void newRepositoryInitializer(RepositoryInitializer ri);
    }

}
