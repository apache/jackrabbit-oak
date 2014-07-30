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

import org.apache.jackrabbit.oak.api.Tree;

/**
 * Extension of the {@link PermissionProvider} interface that allows it to be
 * used in combination with other provider implementations.
 */
public interface AggregatedPermissionProvider extends PermissionProvider {

    /**
     * Name of the configuration option that specifies the {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.ControlFlag}
     * of this provider instance.
     */
    String PARAM_CONTROL_FLAG = "controlFlag";

    @Nonnull
    ControlFlag getFlag();

    boolean handles(@Nonnull String path, @Nonnull String jcrAction);

    boolean handles(@Nonnull Tree tree);

    boolean handles(@Nonnull Tree tree, long permission);

    boolean handles(@Nonnull TreePermission treePermission, long permission);

    boolean handlesRepositoryPermissions();
}