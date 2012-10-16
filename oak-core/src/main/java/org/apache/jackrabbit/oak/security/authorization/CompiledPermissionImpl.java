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
package org.apache.jackrabbit.oak.security.authorization;

import java.security.Principal;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.CompiledPermissions;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;

/**
 * TODO
 */
class CompiledPermissionImpl implements CompiledPermissions {

    CompiledPermissionImpl(Set<Principal> principals) {

    }

    @Override
    public boolean canRead(Tree tree) {
        // TODO
        return true;
    }

    @Override
    public boolean canRead(Tree tree, PropertyState property) {
        // TODO
        return true;
    }

    @Override
    public boolean isGranted(int permissions) {
        // TODO
        return false;
    }

    @Override
    public boolean isGranted(Tree tree, int permissions) {
        // TODO
        return (permissions == Permissions.READ_NODE);
    }

    @Override
    public boolean isGranted(Tree parent, PropertyState property, int permissions) {
        // TODO
        return (permissions == Permissions.READ_PROPERTY);
    }

}
