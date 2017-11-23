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
package org.apache.jackrabbit.oak.upgrade.nodestate;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.migration.AbstractDecoratedNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.document.secondary.DelegatingDocumentNodeState.PROP_LAST_REV;
import static org.apache.jackrabbit.oak.plugins.document.secondary.DelegatingDocumentNodeState.PROP_REVISION;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

public class MetadataExposingNodeState extends AbstractDecoratedNodeState {

    private final List<PropertyState> metadataProperties;

    public MetadataExposingNodeState(AbstractDocumentNodeState documentNodeState) {
        super(documentNodeState);

        metadataProperties = new ArrayList<>(2);
        if (PathUtils.denotesRoot(documentNodeState.getPath())) {
            metadataProperties.add(createProperty(PROP_REVISION, documentNodeState.getRootRevision().asString()));
        }
        metadataProperties.add(createProperty(PROP_LAST_REV, documentNodeState.getLastRevision().asString()));
    }

    @Nonnull
    @Override
    protected Iterable<PropertyState> getNewPropertyStates() {
        return metadataProperties;
    }

    @Nonnull
    @Override
    protected NodeState decorateChild(@Nonnull String name, @Nonnull NodeState delegateChild) {
        return wrap(delegateChild);
    }

    @Override
    protected PropertyState decorateProperty(@Nonnull PropertyState delegatePropertyState) {
        return delegatePropertyState;
    }

    public static NodeState wrap(NodeState wrapped) {
        if (wrapped instanceof AbstractDocumentNodeState) {
            return new MetadataExposingNodeState((AbstractDocumentNodeState) wrapped);
        } else if (wrapped instanceof MetadataExposingNodeState) {
            return wrapped;
        } else if (wrapped instanceof AbstractDecoratedNodeState) {
            NodeState unwrapped = wrapped;
            for (int i = 0; i < 10; i++) {
                if (unwrapped instanceof AbstractDecoratedNodeState) {
                    unwrapped = ((AbstractDecoratedNodeState) unwrapped).getDelegate();
                } else {
                    break;
                }
            }
            if (unwrapped instanceof AbstractDocumentNodeState) {
                return unwrapped;
            }
        }
        throw new IllegalArgumentException();
    }

}
