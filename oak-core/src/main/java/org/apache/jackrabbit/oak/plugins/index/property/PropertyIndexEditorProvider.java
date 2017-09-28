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
package org.apache.jackrabbit.oak.plugins.index.property;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

/**
 * Service that provides PropertyIndex based editors.
 * 
 * @see PropertyIndexEditor
 * @see IndexEditorProvider
 * 
 */
@Component(
        service = IndexEditorProvider.class,
        property = IndexConstants.TYPE_PROPERTY_NAME + "=property")
public class PropertyIndexEditorProvider implements IndexEditorProvider {

    public static final String TYPE = "property";

    @Reference
    private MountInfoProvider mountInfoProvider = Mounts.defaultMountInfoProvider();

    @Override
    public Editor getIndexEditor(
            @Nonnull String type, @Nonnull NodeBuilder definition, @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback) {
        if (TYPE.equals(type)) {
            return new PropertyIndexEditor(definition, root, callback, mountInfoProvider);
        }
        return null;
    }

    public PropertyIndexEditorProvider with(MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
        return this;
    }
}