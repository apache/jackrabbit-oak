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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.JcrNameParser;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
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
    public PropertyExistenceImpl getPropertyExistence() {
        return null;
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        return Collections.singleton(selector);
    }

    @Override
    public PropertyValue currentProperty() {
        String path = selector.currentPath();
        if (path == null) {
            return null;
        }
        String name = PathUtils.getName(path);
        // TODO reverse namespace remapping?
        return PropertyValues.newName(name);
    }

    @Override
    public void restrict(FilterImpl f, Operator operator, PropertyValue v) {
        if (v == null) {
            return;
        }
        if (operator == Operator.NOT_EQUAL && v != null) {
            // not supported
            return;
        }
        String name = getName(query, v);
        if (name != null && f.getSelector().equals(selector)
                && NodeNameImpl.supportedOperator(operator)) {
            String localName = NodeLocalNameImpl.getLocalName(name);
            f.restrictProperty(QueryConstants.RESTRICTION_LOCAL_NAME,
                    operator, PropertyValues.newString(localName));
        }
    }
    
    @Override
    public void restrictList(FilterImpl f, List<PropertyValue> list) {
        // optimizations of type "NAME(..) IN(A, B)" are not supported
    }

    @Override
    public String getFunction(SelectorImpl s) {
        if (!s.equals(selector)) {
            return null;
        }
        return "@" + QueryConstants.RESTRICTION_NAME;
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return s.equals(selector);
    }

    /**
     * Validate that the given value can be converted to a JCR name, and
     * return the name.
     *
     * @param v the value
     * @return name value, or {@code null} if the value can not be converted
     */
    static String getName(QueryImpl query, PropertyValue v) {
        // TODO correctly validate JCR names - see JCR 2.0 spec 3.2.4 Naming Restrictions
        switch (v.getType().tag()) {
        case PropertyType.DATE:
        case PropertyType.DECIMAL:
        case PropertyType.DOUBLE:
        case PropertyType.LONG:
        case PropertyType.BOOLEAN:
            return null;
        }
        String name = v.getValue(Type.NAME);
        // Name escaping (convert _x0020_ to space)
        name = ISO9075.decode(name);
        // normalize paths (./name > name)
        if (query.getNamePathMapper() != null) {
            String mappedName = query.getNamePathMapper().getOakPath(name);
            if (mappedName == null) {
                throw new IllegalArgumentException("Not a valid JCR name: " + name);
            }
            name = mappedName;
        }
        if (PathUtils.isAbsolute(name)) {
            throw new IllegalArgumentException("Not a valid JCR name: "
                    + name + " (absolute paths are not names)");
        } else if (PathUtils.getDepth(name) > 1) {
            throw new IllegalArgumentException("Not a valid JCR name: "
                    + name + " (relative path with depth > 1 are not names)");
        } else if (name.startsWith("[") && !name.endsWith("]")) {
            return null;
        } else if (!JcrNameParser.validate(name)) {
            return null;
        }
        return name;
    }

    static boolean supportedOperator(Operator o) {
        return o == Operator.EQUAL || o == Operator.LIKE;
    }

    @Override
    int getPropertyType() {
        return PropertyType.NAME;
    }

    @Override
    public DynamicOperandImpl createCopy() {
        return new NodeNameImpl(selectorName);
    }
    
    @Override
    public OrderEntry getOrderEntry(SelectorImpl s, OrderingImpl o) {
        if (!s.equals(selector)) {
            // ordered by a different selector
            return null;
        }
        return new OrderEntry(
                QueryConstants.RESTRICTION_NAME, 
            Type.STRING, 
            o.isDescending() ? 
            OrderEntry.Order.DESCENDING : OrderEntry.Order.ASCENDING);
    }

}
