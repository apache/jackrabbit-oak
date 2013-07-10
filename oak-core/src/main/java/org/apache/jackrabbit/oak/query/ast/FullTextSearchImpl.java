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

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

import java.text.ParseException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;

/**
 * A fulltext "contains(...)" condition.
 */
public class FullTextSearchImpl extends ConstraintImpl {

    private final String selectorName;
    private final String relativePath;
    private final String propertyName;
    private final StaticOperandImpl fullTextSearchExpression;
    private SelectorImpl selector;

    public FullTextSearchImpl(
            String selectorName, String propertyName,
            StaticOperandImpl fullTextSearchExpression) {
        this.selectorName = selectorName;

        int slash = -1;
        if (propertyName != null) {
            slash = propertyName.lastIndexOf('/');
        }
        if (slash == -1) {
            this.relativePath = null;
        } else {
            this.relativePath = propertyName.substring(0, slash);
            propertyName = propertyName.substring(slash + 1);
        }

        if (propertyName == null || "*".equals(propertyName)) {
            this.propertyName = null;
        } else {
            this.propertyName = propertyName;
        }

        this.fullTextSearchExpression = fullTextSearchExpression;
    }

    public StaticOperandImpl getFullTextSearchExpression() {
        return fullTextSearchExpression;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("contains(");
        builder.append(quote(selectorName));
        builder.append('.');
        String propertyName = this.propertyName;
        if (propertyName == null) {
            propertyName = "*";
        }
        if (relativePath != null) {
            propertyName = relativePath + "/" + propertyName;
        }
        builder.append(quote(propertyName));
        builder.append(", ");
        builder.append(getFullTextSearchExpression());
        builder.append(')');
        return builder.toString();
    }
    
    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        if (propertyName == null) {
            return Collections.emptySet();
        }
        return Collections.singleton(new PropertyExistenceImpl(selector, selectorName, propertyName));
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        return Collections.singleton(selector);
    }
    
    @Override 
    public Map<DynamicOperandImpl, Set<StaticOperandImpl>> getInMap() {
        return Collections.emptyMap();
    }    

    @Override
    public boolean evaluate() {
        if (selector.index instanceof FulltextQueryIndex) {
            return true;
        }
        
        StringBuilder buff = new StringBuilder();
        if (relativePath == null && propertyName != null) {
            PropertyValue p = selector.currentProperty(propertyName);
            if (p == null) {
                return false;
            }
            appendString(buff, p);
        } else {
            String path = selector.currentPath();
            if (relativePath != null) {
                path = PathUtils.concat(path, relativePath);
            }

            Tree tree = getTree(path);
            if (tree == null || !tree.exists()) {
                return false;
            }

            if (propertyName != null) {
                PropertyState p = tree.getProperty(propertyName);
                if (p == null) {
                    return false;
                }
                appendString(buff, PropertyValues.create(p));
            } else {
                for (PropertyState p : tree.getProperties()) {
                    appendString(buff, PropertyValues.create(p));
                }
            }
        }
        // TODO fulltext conditions: need a way to disable evaluation
        // if a fulltext index is used, to avoid filtering too much
        // (we don't know what exact options are used in the fulltext index)
        // (stop word, special characters,...)
        PropertyValue v = fullTextSearchExpression.currentValue();
        try {
            FullTextExpression expr = FullTextParser.parse(v.getValue(Type.STRING));
            return expr.evaluate(buff.toString());
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid expression: " + fullTextSearchExpression, e);
        }
    }

    private static void appendString(StringBuilder buff, PropertyValue p) {
        if (p.isArray()) {
            for (String v : p.getValue(STRINGS)) {
                buff.append(v).append(' ');
            }
        } else {
            buff.append(p.getValue(STRING)).append(' ');
        }
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public void restrict(FilterImpl f) {
        if (propertyName != null) {
            if (f.getSelector() == selector) {
                f.restrictProperty(propertyName, Operator.NOT_EQUAL, null);
            }
        }
        f.restrictFulltextCondition(fullTextSearchExpression.currentValue().getValue(Type.STRING));
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        if (s == selector) {
            selector.restrictSelector(this);
        }
    }

}
