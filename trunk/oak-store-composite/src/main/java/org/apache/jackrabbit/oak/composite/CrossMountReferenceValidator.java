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
package org.apache.jackrabbit.oak.composite;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.Multiplexers;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.api.CommitFailedException.INTEGRITY;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

public class CrossMountReferenceValidator extends DefaultValidator {

    private final Logger LOG = LoggerFactory.getLogger(CrossMountReferenceValidator.class);

    /** Parent editor, or {@code null} if this is the root editor. */
    private final CrossMountReferenceValidator parent;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    /** UUID -> referencable node path */
    private final Map<String, String> newReferencableNodes;

    /** UUID -> referencing node */
    private final Multimap<String, String> newReferences;

    private final MountInfoProvider mip;

    private final NodeState uuidDefinition;

    private final Set<IndexStoreStrategy> uuidStores;

    private final boolean failOnDetection;

    private CrossMountReferenceValidator(CrossMountReferenceValidator parent, String name) {
        this.name = name;
        this.parent = parent;
        this.path = null;
        this.mip = parent.mip;
        this.newReferencableNodes = parent.newReferencableNodes;
        this.newReferences = parent.newReferences;
        this.uuidDefinition = parent.uuidDefinition;
        this.uuidStores = parent.uuidStores;
        this.failOnDetection = parent.failOnDetection;
    }

    public CrossMountReferenceValidator(NodeState root, MountInfoProvider mip, boolean failOnDetection) {
        this.name = null;
        this.parent = null;
        this.path = "/";
        this.mip = mip;
        this.newReferencableNodes = newHashMap();
        this.newReferences = ArrayListMultimap.create();
        this.uuidDefinition = root.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("uuid");
        this.uuidStores = Multiplexers.getStrategies(true, mip, uuidDefinition, INDEX_CONTENT_NODE_NAME);
        this.failOnDetection = failOnDetection;
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (parent != null) {
            return;
        }
        for (Map.Entry<String, Collection<String>> e : newReferences.asMap().entrySet()) {
            String uuid = e.getKey();
            String passivePath = getPathByUuid(uuid);
            if (passivePath == null) {
                LOG.warn("Can't find path for the UUID {}", uuid);
            }
            Mount m1 = mip.getMountByPath(passivePath);

            for (String activePath : e.getValue()) {
                Mount m2 = mip.getMountByPath(activePath);
                if (!m1.equals(m2)) {
                    if (failOnDetection) {
                        throw new CommitFailedException(INTEGRITY, 1,
                                "Unable to reference the node [" + passivePath + "] from node [" + activePath + "]. Referencing across the mounts is not allowed.");
                    } else {
                        LOG.warn("Detected a cross-mount reference: {} -> {}", activePath, passivePath);
                    }
                }
            }
        }
    }

    private String getPathByUuid(String uuid) {
        if (newReferencableNodes.containsKey(uuid)) {
            return newReferencableNodes.get(uuid);
        }
        for (IndexStoreStrategy store : uuidStores) {
            for (String path : store.query(Filter.EMPTY_FILTER, null, uuidDefinition, of(uuid))) {
                return path;
            }
        }
        return null;
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        checkProperty(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        checkProperty(after);
    }

    private void checkProperty(PropertyState property) {
        Type<?> type = property.getType();
        if (type == Type.REFERENCE) {
            newReferences.put(property.getValue(Type.REFERENCE), getPath());
        } else if (type == Type.REFERENCES) {
            for (String r : property.getValue(Type.REFERENCES)) {
                newReferences.put(r, getPath());
            }
        } else if (type == Type.STRING && JCR_UUID.equals(property.getName())) {
            newReferencableNodes.put(property.getValue(Type.STRING), getPath());
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        return new CrossMountReferenceValidator(this, name);
    }

    @Override
    public Validator childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        return new CrossMountReferenceValidator(this, name);
    }

    /**
     * Returns the path of this node, building it lazily when first requested.
     */
    private String getPath() {
        if (path == null) {
            path = concat(parent.getPath(), name);
        }
        return path;
    }

}
