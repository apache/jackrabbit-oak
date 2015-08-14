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
package org.apache.jackrabbit.oak.plugins.index.diffindex;

import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * DiffCollector that looks for UUID properties
 * 
 */
public class UUIDDiffCollector extends BaseDiffCollector {

    private String uuid;

    public UUIDDiffCollector(NodeState before, NodeState after) {
        super(before, after);
    }

    @Override
    public void collect(Filter filter) {
        uuid = null;
        Filter.PropertyRestriction restriction = filter
                .getPropertyRestriction("jcr:uuid");
        if (restriction == null || restriction.isLike
                || !restriction.firstIncluding || !restriction.lastIncluding
                || !restriction.first.equals(restriction.last)) {
            init = true;
            return;
        }
        uuid = restriction.first.toString();
        super.collect(filter);
    }
    
    private static String extractUuidFromFilter(Filter filter) {
        Filter.PropertyRestriction restriction = filter
                .getPropertyRestriction("jcr:uuid");
        return restriction.first.toString();
    }
    
    @Override
    public Set<String> getResults(Filter filter) {
        if (init && !extractUuidFromFilter(filter).equals(uuid)) {
            throw new IllegalArgumentException("UUID does not match ");
        }
        return super.getResults(filter);
    }

    @Override
    protected boolean match(NodeState state, Filter filter) {
        if (uuid == null) {
            return false;
        }
        PropertyState propertyState = state.getProperty("jcr:uuid");
        return propertyState != null
                && uuid.equals(propertyState.getValue(Type.STRING));
    }

    @Override
    protected boolean isUnique() {
        return true;
    }

}
