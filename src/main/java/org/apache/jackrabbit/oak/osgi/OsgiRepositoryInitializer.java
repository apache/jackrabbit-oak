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

import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.framework.ServiceReference;

/**
 * Implements a service tracker that keeps track of all
 * {@link RepositoryInitializer}s in the system and calls the available
 * method once the micro kernel is available.
 */
public class OsgiRepositoryInitializer
        extends AbstractServiceTracker<RepositoryInitializer>
        implements RepositoryInitializer {

    /**
     * The reference to the micro kernel once available.
     */
    private volatile NodeStore store;

    public OsgiRepositoryInitializer() {
        super(RepositoryInitializer.class);
    }

    @Override
    public void initialize(NodeStore store) {
        this.store = store;
        if (store != null) {
            for (RepositoryInitializer mki : getServices()) {
                mki.initialize(store);
            }
        }
    }

    @Override
    public Object addingService(ServiceReference reference) {
        RepositoryInitializer mki =
                (RepositoryInitializer) super.addingService(reference);
        NodeStore store = this.store;
        if (store != null) {
            //TODO index added content
            mki.initialize(store);
        }
        return mki;
    }

}
