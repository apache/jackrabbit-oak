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
package org.apache.jackrabbit.oak.jcr.lock;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.jcr.Node;
import javax.jcr.lock.Lock;

public final class LockImpl implements Lock {

    private final Node node;

    private final String userID;

    private final boolean isDeep;

    public LockImpl(Node node, String userID, boolean isDeep) {
        this.node = checkNotNull(node);
        this.userID = userID;
        this.isDeep = isDeep;
    }

    @Override
    public Node getNode() {
        return node;
    }

    @Override
    public String getLockOwner() {
        return userID;
    }

    @Override
    public boolean isDeep() {
        return isDeep;
    }

    @Override
    public String getLockToken() {
        return null;
    }

    @Override
    public long getSecondsRemaining() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean isLive() {
        return true;
    }

    @Override
    public boolean isSessionScoped() {
        return true;
    }

    @Override
    public boolean isLockOwningSession() {
        return true;
    }

    @Override
    public void refresh() {
    }

}
