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
package org.apache.jackrabbit.oak.security.authorization.permission;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class InternalTreePermission implements TreePermission {

    private static final Logger log = LoggerFactory.getLogger(InternalTreePermission.class);

    static final TreePermission INSTANCE = new InternalTreePermission();

    private InternalTreePermission() {}

    @Override
    public @NotNull TreePermission getChildPermission(@NotNull String childName, @NotNull NodeState childState) {
        return INSTANCE;
    }

    @Override
    public boolean canRead() {
        return EMPTY.canRead();
    }

    @Override
    public boolean canRead(@NotNull PropertyState property) {
        return EMPTY.canRead(property);
    }

    @Override
    public boolean canReadAll() {
        return EMPTY.canReadAll();
    }

    @Override
    public boolean canReadProperties() {
        return EMPTY.canReadProperties();
    }

    @Override
    public boolean isGranted(long permissions) {
        return EMPTY.isGranted(permissions);
    }

    @Override
    public boolean isGranted(long permissions, @NotNull PropertyState property) {
        return EMPTY.isGranted(permissions, property);
    }
}