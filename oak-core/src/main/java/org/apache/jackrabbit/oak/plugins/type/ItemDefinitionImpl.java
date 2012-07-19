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
package org.apache.jackrabbit.oak.plugins.type;

import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_ABORT;
import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_COMPUTE;
import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_COPY;
import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_IGNORE;
import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_INITIALIZE;
import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_VERSION;

import java.util.List;

import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NameMapper;

/**
 * <pre>
 * [nt:{propertyDefinition,childNodeDefinition}]
 * - jcr:name (NAME) protected 
 * - jcr:autoCreated (BOOLEAN) protected mandatory
 * - jcr:mandatory (BOOLEAN) protected mandatory
 * - jcr:onParentVersion (STRING) protected mandatory
 *     < 'COPY', 'VERSION', 'INITIALIZE', 'COMPUTE', 'IGNORE', 'ABORT'
 * - jcr:protected (BOOLEAN) protected mandatory
 *   ...
 * </pre>
 */

class ItemDefinitionImpl implements ItemDefinition {

    private final NodeType type;

    private final NameMapper mapper;

    private final Tree tree;

    protected ItemDefinitionImpl(
            NodeType type, NameMapper mapper, Tree tree) {
        this.type = type;
        this.mapper = mapper;
        this.tree = tree;
    }

    protected boolean getBoolean(String name, boolean defaultValue) {
        PropertyState property = tree.getProperty(name);
        if (property != null && !property.isArray()) {
            return property.getValue().getBoolean();
        } else {
            return defaultValue;
        }
    }

    protected String getString(String name, String defaultValue) {
        PropertyState property = tree.getProperty(name);
        if (property != null && !property.isArray()) {
            return property.getValue().getString();
        } else {
            return defaultValue;
        }
    }

    protected String[] getStrings(String name, String[] defaultValues) {
        PropertyState property = tree.getProperty(name);
        if (property != null) {
            List<CoreValue> values = property.getValues();
            String[] strings = new String[values.size()];
            for (int i = 0; i < strings.length; i++) {
                strings[i] = values.get(i).getString();
            }
            return strings;
        } else {
            return defaultValues;
        }
    }

    protected String getName(String name, String defaultValue) {
        PropertyState property = tree.getProperty(name);
        if (property != null && !property.isArray()) {
            return mapper.getJcrName(property.getValue().getString());
        } else {
            return defaultValue;
        }
    }

    protected String[] getNames(String name, String... defaultValues) {
        String[] strings = getStrings(name, defaultValues);
        for (int i = 0; i < strings.length; i++) {
            strings[i] = mapper.getJcrName(strings[i]);
        }
        return strings;
    }

    @Override
    public NodeType getDeclaringNodeType() {
        return type;
    }

    @Override
    public String getName() {
        return getName("jcr:name", "*");
    }

    @Override
    public boolean isAutoCreated() {
        return getBoolean("jcr:autoCreated", false);
    }

    @Override
    public boolean isMandatory() {
        return getBoolean("jcr:mandatory", false);
    }

    @Override
    public int getOnParentVersion() {
        String opv = getString("jcr:onParentVersion", ACTIONNAME_COPY);
        if (ACTIONNAME_ABORT.equalsIgnoreCase(opv)) {
            return OnParentVersionAction.ABORT;
        } else if (ACTIONNAME_COMPUTE.equalsIgnoreCase(opv)) {
            return OnParentVersionAction.COMPUTE;
        } else if (ACTIONNAME_IGNORE.equalsIgnoreCase(opv)) {
            return OnParentVersionAction.IGNORE;
        } else if (ACTIONNAME_INITIALIZE.equalsIgnoreCase(opv)) {
            return OnParentVersionAction.INITIALIZE;
        } else if (ACTIONNAME_VERSION.equalsIgnoreCase(opv)) {
            return OnParentVersionAction.VERSION;
        } else {
            return OnParentVersionAction.COPY;
        }
    }

    @Override
    public boolean isProtected() {
        return getBoolean("jcr:protected", false);
    }

}
