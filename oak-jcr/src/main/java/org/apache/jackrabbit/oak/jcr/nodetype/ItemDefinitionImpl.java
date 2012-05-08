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

    private final ItemDefinitionDelegate dlg;

    private final NodeType type;

    protected final NameMapper mapper;

    protected ItemDefinitionImpl(NodeType type, NameMapper mapper, ItemDefinitionDelegate delegate) {
        this.dlg = delegate;
        this.type = type;
        this.mapper = mapper;
    }

    @Override
    public NodeType getDeclaringNodeType() {
        return type;
    }

    @Override
    public String getName() {
        return mapper.getJcrName(dlg.getName());
    }

    @Override
    public boolean isAutoCreated() {
        return dlg.isAutoCreated();
    }

    @Override
    public boolean isMandatory() {
        return dlg.isMandatory();
    }

    @Override
    public int getOnParentVersion() {
        return dlg.getOnParentVersion();
    }

    @Override
    public boolean isProtected() {
        return dlg.isProtected();
    }
}
