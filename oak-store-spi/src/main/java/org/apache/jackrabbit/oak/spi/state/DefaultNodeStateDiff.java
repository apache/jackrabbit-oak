/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.spi.state;

import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * Node state diff handler that by default does nothing. Useful as a base
 * class for more complicated diff handlers that can safely ignore one or
 * more types of changes.
 */
public class DefaultNodeStateDiff implements NodeStateDiff {

    @Override
    public boolean propertyAdded(PropertyState after) {
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        return true;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        return true;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        return true;
    }

}
