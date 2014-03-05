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

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class OrderedPropertyIndexEditor extends PropertyIndexEditor {
    private static final Logger log = LoggerFactory.getLogger(OrderedPropertyIndexEditor.class);
    private static final IndexStoreStrategy ORDERED_MIRROR = new OrderedContentMirrorStoreStrategy();

    private final Set<String> propertyNames;

    private boolean properlyConfigured;

    public OrderedPropertyIndexEditor(NodeBuilder definition, NodeState root,
                                      IndexUpdateCallback callback) {
        super(definition, root, callback);

        Set<String> pns = null;

        PropertyState names = definition.getProperty(IndexConstants.PROPERTY_NAMES);
        if (names != null) {
            String value = names.getValue(Type.NAME, 0);
            if (Strings.isNullOrEmpty(value)) {
                log.warn("Empty value passed as propertyNames. Index not properly configured. Ignoring.");
            } else {
                if (names.isArray()) {
                    log.warn("Only single value supported. '{}' only will be used.", value);
                }
                pns = Collections.singleton(value);
                this.properlyConfigured = true;
            }
        }

        this.propertyNames = pns;
    }

    OrderedPropertyIndexEditor(OrderedPropertyIndexEditor parent, String name) {
        super(parent, name);
        this.propertyNames = parent.getPropertyNames();
    }

    /**
     * Same as {@link PropertyIndexEditor#getStrategy(boolean)} but ignores the boolean flag.
     * 
     * @return the proper index strategy
     */
    @Override
    IndexStoreStrategy getStrategy(boolean unique) {
        return ORDERED_MIRROR;
    }

    public boolean isProperlyConfigured() {
        return properlyConfigured;
    }

    @Override
    Set<String> getPropertyNames() {
        return propertyNames;
    }

    @Override
    PropertyIndexEditor getChildIndexEditor(@Nonnull PropertyIndexEditor parent, 
                                            @Nonnull String name) {
        return new OrderedPropertyIndexEditor(this, name);
    }

    @Override
    public void enter(NodeState before, NodeState after) {
        log.debug("enter() - before: {} - after: {}", before, after);
        super.enter(before, after);
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        log.debug("leave() - before: {} - after: {}", before, after);
        super.leave(before, after);
    }

    @Override
    public void propertyAdded(PropertyState after) {
        log.debug("propertyAdded() - after: {}", after);
        super.propertyAdded(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        log.debug("propertyChanged() - before: {} - after: {}", before, after);
        super.propertyChanged(before, after);
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        log.debug("propertyDeleted() -  before: {}", before);
        super.propertyDeleted(before);
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        log.debug("childNodeAdded() - name: {} - after: {}", name, after);
        return super.childNodeAdded(name, after);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) {
        log.debug("childNodeChanged() - name: {} - before: {} - after: {}", new Object[] { name,
                                                                                          before,
                                                                                          after });
        return super.childNodeChanged(name, before, after);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) {
        log.debug("childNodeDeleted() - name: {} - before: {}", name, before);
        return super.childNodeDeleted(name, before);
    }
}
