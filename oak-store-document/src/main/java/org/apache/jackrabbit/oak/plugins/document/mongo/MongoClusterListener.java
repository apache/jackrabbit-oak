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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import javax.annotation.CheckForNull;

import com.mongodb.connection.ClusterDescription;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListenerAdapter;

/**
 * Remembers the current {@link ClusterDescription} whenever it changes.
 */
class MongoClusterListener
        extends ClusterListenerAdapter
        implements ClusterDescriptionProvider {

    private volatile ClusterDescription description;

    @CheckForNull
    public ClusterDescription getClusterDescription() {
        return description;
    }

    @Override
    public void clusterDescriptionChanged(ClusterDescriptionChangedEvent event) {
        description = event.getNewDescription();
    }
}
