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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MongoClusterListener implements ClusterListener {

    // Sometimes we need to wait a few seconds in case the connection was just created, the listener
    // didn't have time to receive the description from the cluster. This latch is used to check
    // if the connection was properly initialized.
    private final CountDownLatch LATCH;
    private final int LATCH_AWAIT_TIMEOUT = 15;
    private boolean replicaSet = false;
    private ServerAddress serverAddress;
    private ServerAddress primaryAddress;

    public MongoClusterListener() {
        LATCH = new CountDownLatch(1);
    }

    @Nullable
    public ServerAddress getServerAddress() {
        waitForLatch();
        return serverAddress;
    }

    @Nullable
    public ServerAddress getPrimaryAddress() {
        waitForLatch();
        return primaryAddress;
    }

    public boolean isReplicaSet() {
        waitForLatch();
        return replicaSet;
    }

    @Override
    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
        for (ServerDescription sd : event.getNewDescription().getServerDescriptions()) {
            if (sd.getState() == ServerConnectionState.CONNECTED) {
                serverAddress = sd.getAddress();
                primaryAddress = new ServerAddress(sd.getPrimary());
                if (sd.isReplicaSetMember()) {
                    // Can't assign directly the result of the function because in some cases the cluster
                    // type is UNKNOWN, mainly when the cluster is changing it's PRIMARY.
                    replicaSet = true;
                }
                LATCH.countDown();
            }
        }
    }

    private void waitForLatch() {
        try {
            LATCH.await(LATCH_AWAIT_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}
    }
}
