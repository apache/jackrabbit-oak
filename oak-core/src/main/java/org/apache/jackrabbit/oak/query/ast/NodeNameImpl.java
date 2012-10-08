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
package org.apache.jackrabbit.oak.query.ast;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.util.ISO9075;

/**
 * The function "name(..)".
 */
public class NodeNameImpl extends DynamicOperandImpl {

    private final String selectorName;
    private SelectorImpl selector;

    public NodeNameImpl(String selectorName) {
        this.selectorName = selectorName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "name(" + quote(selectorName) + ')';
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public boolean supportsRangeConditions() {
        return false;
    }

    @Override
    public PropertyState currentProperty() {
        String path = selector.currentPath();
        // Name escaping (convert space to _x0020_)
        String name = ISO9075.encode(PathUtils.getName(path));
        return PropertyStates.nameProperty("NAME", name);
    }

    @Override
    public void restrict(FilterImpl f, Operator operator, CoreValue v) {
        if (v == null) {
            return;
        }
        if (!isName(v)) {
            throw new IllegalArgumentException("Invalid name value: " + v.toString());
        }
        String path = v.getString();
        // Name escaping (convert _x0020_ to space)
        path = decodeName(path);
        if (PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("NAME() comparison with absolute path are not allowed: " + path);
        }
        if (PathUtils.getDepth(path) > 1) {
            throw new IllegalArgumentException("NAME() comparison with relative path are not allowed: " + path);
        }
        // TODO support NAME(..) index conditions
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return s == selector;
    }

    private String decodeName(String path) {
        // Name escaping (convert _x0020_ to space)
        path = ISO9075.decode(path);
        // normalize paths (./name > name)
        path = query.getOakPath(path);
        return path;
    }

    /**
     * Validate that the given value can be converted to a JCR name.
     *
     * @param v the value
     * @return true if it can be converted
     */
    private static boolean isName(CoreValue v) {
        // TODO correctly validate JCR names - see JCR 2.0 spec 3.2.4 Naming Restrictions
        switch (v.getType()) {
        case PropertyType.DATE:
        case PropertyType.DECIMAL:
        case PropertyType.DOUBLE:
        case PropertyType.LONG:
        case PropertyType.BOOLEAN:
            return false;
        }
        String n = v.getString();
        if (n.startsWith("[") && !n.endsWith("]")) {
            return false;
        }
        return true;
    }

}
