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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TreePermission... TODO
 */
public interface TreePermission {

    TreePermission getChildPermission(String childName, NodeState childState);

    boolean canRead();

    boolean canRead(@Nonnull PropertyState property);

    boolean canReadAll();

    boolean canReadProperties();

    boolean isGranted(long permissions);

    boolean isGranted(long permissions, @Nonnull PropertyState property);

    TreePermission EMPTY = new TreePermission() {
        @Override
        public TreePermission getChildPermission(String childName, NodeState childState) {
            return EMPTY;
        }

        @Override
        public boolean canRead() {
            return false;
        }

        @Override
        public boolean canRead(@Nonnull PropertyState property) {
            return false;
        }

        @Override
        public boolean canReadAll() {
            return false;
        }

        @Override
        public boolean canReadProperties() {
            return false;
        }

        @Override
        public boolean isGranted(long permissions) {
            return false;
        }

        @Override
        public boolean isGranted(long permissions, @Nullable PropertyState property) {
            return false;
        }
    };

    TreePermission ALL = new TreePermission() {
        @Override
        public TreePermission getChildPermission(String childName, NodeState childState) {
            return ALL;
        }

        @Override
        public boolean canRead() {
            return true;
        }

        @Override
        public boolean canRead(@Nonnull PropertyState property) {
            return true;
        }

        @Override
        public boolean canReadAll() {
            return true;
        }

        @Override
        public boolean canReadProperties() {
            return true;
        }

        @Override
        public boolean isGranted(long permissions) {
            return true;
        }

        @Override
        public boolean isGranted(long permissions, @Nullable PropertyState property) {
            return true;
        }
    };
}