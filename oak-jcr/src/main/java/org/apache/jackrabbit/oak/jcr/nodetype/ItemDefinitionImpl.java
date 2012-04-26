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
package org.apache.jackrabbit.oak.jcr.nodetype;

import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.oak.namepath.NameMapper;

class ItemDefinitionImpl implements ItemDefinition {

    private final NodeType type;

    protected final NameMapper mapper;

    private final String name;

    private final boolean autoCreated;

    private final boolean mandatory;

    private final int onParentRevision;

    private final boolean isProtected;

    protected ItemDefinitionImpl(
            NodeType type, NameMapper mapper, String name, boolean autoCreated,
            boolean mandatory, int onParentRevision, boolean isProtected) {
        this.type = type;
        this.mapper = mapper;
        this.name = name;
        this.autoCreated = autoCreated;
        this.mandatory = mandatory;
        this.onParentRevision = onParentRevision;
        this.isProtected = isProtected;
    }

    @Override
    public NodeType getDeclaringNodeType() {
        return type;
    }

    @Override
    public String getName() {
        return mapper.getJcrName(name);
    }

    @Override
    public boolean isAutoCreated() {
        return autoCreated;
    }

    @Override
    public boolean isMandatory() {
        return mandatory;
    }

    @Override
    public int getOnParentVersion() {
        return onParentRevision;
    }

    @Override
    public boolean isProtected() {
        return isProtected;
    }


}
