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
package org.apache.jackrabbit.oak.security.authorization.composite;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;

public class MultiplexingPermissionProvider extends CompositePermissionProvider {

    public MultiplexingPermissionProvider(Root root, List<AggregatedPermissionProvider> pps, Context acContext) {
        super(root, pps, acContext, CompositionType.OR);
    }

    @Nonnull
    public static String getPermissionRootName(@Nonnull Mount mount, @Nonnull String workspace) {
        if (mount.isDefault()) {
            return workspace;
        } else {
            return mount.getPathFragmentName() + "-" + workspace;
        }
    }

}
