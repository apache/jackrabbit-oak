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
import javax.jcr.version.OnParentVersionAction;

class ItemDefinitionTemplateImpl implements ItemDefinition {

    private String name = null;

    private boolean isAutoCreated = false;

    private boolean isMandatory = false;

    private boolean isProtected = false;

    private int onParentVersion = OnParentVersionAction.COPY;

    @Override
    public NodeType getDeclaringNodeType() {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean isAutoCreated() {
        return isAutoCreated;
    }

    public void setAutoCreated(boolean isAutoCreated) {
        this.isAutoCreated = isAutoCreated;
    }

    @Override
    public boolean isMandatory() {
        return isMandatory;
    }

    public void setMandatory(boolean isMandatory) {
        this.isMandatory = isMandatory;
    }

    @Override
    public boolean isProtected() {
        return isProtected;
    }

    public void setProtected(boolean isProtected) {
        this.isProtected = isProtected;
    }

    @Override
    public int getOnParentVersion() {
        return onParentVersion;
    }

    public void setOnParentVersion(int opv) {
        onParentVersion = opv;
    }

}