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
package org.apache.jackrabbit.oak.plugins.migration;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class NameFilteringNodeState extends AbstractDecoratedNodeState {

    private static final Logger LOG = LoggerFactory.getLogger(NameFilteringNodeState.class);

    private static final int NODE_NAME_LIMIT = 150;
    /**
     * Max character size in bytes in UTF8 = 4. Therefore if the number of characters is smaller
     * than NODE_NAME_LIMIT / 4 we don't need to count bytes.
     */
    private static final int SAFE_NODE_NAME_LENGTH = NODE_NAME_LIMIT / 4;

    public static NodeState wrap(final NodeState delegate) {
        return new NameFilteringNodeState(delegate);
    }

    private NameFilteringNodeState(final NodeState delegate) {
        super(delegate);
    }

    @Override
    protected boolean hideChild(@Nonnull final String name, @Nonnull final NodeState delegateChild) {
        if (isNameTooLong(name)) {
            LOG.warn("Node name '{}' too long. Skipping child of {}", name, this);
            return true;
        }
        return super.hideChild(name, delegateChild);
    }

    @Override
    @Nonnull
    protected NodeState decorateChild(@Nonnull final String name, @Nonnull final NodeState delegateChild) {
        return wrap(delegateChild);
    }

    @Override
    protected PropertyState decorateProperty(@Nonnull final PropertyState delegatePropertyState) {
        return fixChildOrderPropertyState(this, delegatePropertyState);
    }

    /**
     * This method checks whether the name is no longer than the maximum node
     * name length supported by the DocumentNodeStore.
     *
     * @param name
     *            to check
     * @return true if the name is longer than {@code org.apache.jackrabbit.oak.plugins.document.util.Utils#NODE_NAME_LIMIT}
     */
    public static boolean isNameTooLong(@Nonnull String name) {
        // OAK-1589: maximum supported length of name for DocumentNodeStore
        // is 150 bytes. Skip the sub tree if the the name is too long
        return name.length() > SAFE_NODE_NAME_LENGTH && name.getBytes(Charsets.UTF_8).length > NODE_NAME_LIMIT;
    }
}
