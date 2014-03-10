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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Index editor for keeping an ordered property index up to date.
 */
public class OrderedPropertyIndexEditor extends PropertyIndexEditor {
    /**
     * the default Ascending ordered StoreStrategy
     */
    static final IndexStoreStrategy ORDERED_MIRROR = new OrderedContentMirrorStoreStrategy();
    
    /**
     * the Descending ordered StoreStrategy
     */
    static final IndexStoreStrategy ORDERED_MIRROR_DESCENDING = new OrderedContentMirrorStoreStrategy(OrderDirection.DESC);

    private static final Logger LOG = LoggerFactory.getLogger(OrderedPropertyIndexEditor.class);
    
    private final Set<String> propertyNames;

    private boolean properlyConfigured;

    private OrderDirection direction = OrderedIndex.DEFAULT_DIRECTION;

    public OrderedPropertyIndexEditor(NodeBuilder definition, NodeState root,
                                      IndexUpdateCallback callback) {
        super(definition, root, callback);

        // configuring propertyNames
        Set<String> pns = null;
        PropertyState names = definition.getProperty(IndexConstants.PROPERTY_NAMES);
        if (names != null) {
            String value = names.getValue(Type.NAME, 0);
            if (Strings.isNullOrEmpty(value)) {
                LOG.warn("Empty value passed as propertyNames. Index not properly configured. Ignoring.");
            } else {
                if (names.isArray()) {
                    LOG.warn("Only single value supported. '{}' only will be used.", value);
                }
                pns = Collections.singleton(value);
                this.properlyConfigured = true;
            }
        }
        this.propertyNames = pns;

        // configuring direction
        String propertyDirection = definition.getString(OrderedIndex.DIRECTION);
        if (propertyDirection == null) {
            LOG.info("Using default direction for sorting: {}", this.direction);
        } else {
            OrderDirection dir = OrderDirection.fromString(propertyDirection);
            if (dir == null) {
                LOG.warn("An unknown direction has been specified for sorting: '{}'. Using default one. {}",
                         propertyDirection, this.direction);
            } else {
                this.direction = dir;
            }
        }
    }

    OrderedPropertyIndexEditor(OrderedPropertyIndexEditor parent, String name) {
        super(parent, name);
        this.propertyNames = parent.getPropertyNames();
        this.direction = parent.getDirection();
    }

    /**
     * Same as {@link PropertyIndexEditor#getStrategy(boolean)} but ignores the boolean flag.
     * 
     * @return the proper index strategy
     */
    @Override
    IndexStoreStrategy getStrategy(boolean unique) {
        IndexStoreStrategy store = ORDERED_MIRROR;
        if (!OrderedIndex.DEFAULT_DIRECTION.equals(getDirection())) {
            store = ORDERED_MIRROR_DESCENDING;
        }
        return store;
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

    /**
     * 
     * @return the direction of the index configuration
     */
    public OrderDirection getDirection() {
        return direction;
    }
}
