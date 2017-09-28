/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.property;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableList;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

/**
 * A provider for property indexes.
 * <br>
 * Even if there are multiple index definitions, there is only actually one
 * PropertyIndex instance, which is used for all indexes.
 * 
 * @see PropertyIndex
 */
@Component(service = QueryIndexProvider.class)
public class PropertyIndexProvider implements QueryIndexProvider {

    @Reference
    private MountInfoProvider mountInfoProvider = Mounts.defaultMountInfoProvider();

    @Override @Nonnull
    public List<QueryIndex> getQueryIndexes(NodeState state) {
        return ImmutableList.<QueryIndex>of(new PropertyIndex(mountInfoProvider));
    }

    public PropertyIndexProvider with(MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
        return this;
    }
}
