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

public class NodeDefinitionDelegate extends ItemDefinitionDelegate {

    private final String[] requiredPrimaryTypeNames;
    private final String defaultPrimaryTypeName;
    private final boolean allowsSameNameSiblings;

    protected NodeDefinitionDelegate(String name, boolean autoCreated, boolean mandatory, int onParentVersion, boolean isProtected,
            String[] requiredPrimaryTypeNames, String defaultPrimaryTypeName, boolean allowsSameNameSiblings) {
        super(name, autoCreated, mandatory, onParentVersion, isProtected);
        this.requiredPrimaryTypeNames = requiredPrimaryTypeNames;
        this.defaultPrimaryTypeName = defaultPrimaryTypeName;
        this.allowsSameNameSiblings = allowsSameNameSiblings;
    }

    public boolean allowsSameNameSiblings() {
        return allowsSameNameSiblings;
    }

    public String getDefaultPrimaryTypeName() {
        return defaultPrimaryTypeName;
    }

    public String[] getRequiredPrimaryTypeNames() {
        return requiredPrimaryTypeNames;
    }
}
