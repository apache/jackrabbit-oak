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
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;

/**
 * A fulltext "contains(...)" condition.
 */
public class FullTextSearchImpl extends ConstraintImpl {

    final String selectorName;
    private final String relativePath;
    final String propertyName;
    final StaticOperandImpl fullTextSearchExpression;
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
    ConstraintImpl not() {
        return new NotFullTextSearchImpl(this);
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
        String pn = propertyName;
        if (pn == null) {
            pn = "*";
        }
        if (relativePath != null) {
            pn = relativePath + "/" + pn;
        }
        builder.append(quote(pn));
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
        String fullName;
        if (relativePath != null) {
            fullName = PathUtils.concat(relativePath, propertyName);
        } else {
            fullName = propertyName;
        }
        return Collections.singleton(new PropertyExistenceImpl(selector, selectorName, fullName));
    }

    @Override
    public FullTextExpression getFullTextConstraint(SelectorImpl s) {
        if (!s.equals(selector)) {
            return null;
        }
        PropertyValue v = fullTextSearchExpression.currentValue();
        try {
            String p = propertyName;
            if (relativePath != null) {
                if (p == null) {
                    p = "*";
                }
                p = PathUtils.concat(relativePath, p);
            }
            String p2 = normalizePropertyName(p);
            String rawText = getRawText(v);
            FullTextExpression e = FullTextParser.parse(p2, rawText);
            return new FullTextContains(p2, rawText, e);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid expression: " + fullTextSearchExpression, e);
        }
    }
    
    String getRawText(PropertyValue v) {
        return v.getValue(Type.STRING);
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        return Collections.singleton(selector);
    }

    /**
     * verify that a property exists in the node. {@code property IS NOT NULL}
     * 
     * @param propertyName the property to check
     * @param selector the selector to work with
     * @return true if the property is there, false otherwise.
     */
    boolean enforcePropertyExistence(String propertyName, SelectorImpl selector) {
        PropertyValue p = selector.currentProperty(propertyName);
        if (p == null) {
            return false;
        }
        return true;
    }
    
    @Override
    public boolean evaluate() {
        // disable evaluation if a fulltext index is used,
        // to avoid running out of memory if the node is large,
        // and because we might not implement all features
        // such as index aggregation
        if (selector.getIndex() instanceof FulltextQueryIndex) {
            // first verify if a property level condition exists and if that
            // condition checks out, this takes out some extra rows from the index
            // aggregation bits
            if (relativePath == null && propertyName != null) {
                return enforcePropertyExistence(propertyName, selector);
            }
            return true;
        }
        // OAK-2050
        if (!query.getSettings().getFullTextComparisonWithoutIndex()) {
            return false;
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
            if (!PathUtils.denotesRoot(path)) {
                appendString(buff,
                        PropertyValues.newString(PathUtils.getName(path)));
            }
            if (relativePath != null) {
                String rp = normalizePath(relativePath);
                path = PathUtils.concat(path, rp);
            }

            Tree tree = selector.getTree(path);
            if (tree == null || !tree.exists()) {
                return false;
            }

            if (propertyName != null) {
                String pn = normalizePropertyName(propertyName);
                PropertyState p = tree.getProperty(pn);
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
        return getFullTextConstraint(selector).evaluate(buff.toString());
    }
    
    @Override
    public boolean evaluateStop() {
        // if a fulltext index is used, then we are fine
        if (selector.getIndex() instanceof FulltextQueryIndex) {
            return false;
        }
        // OAK-2050
        if (!query.getSettings().getFullTextComparisonWithoutIndex()) {
            return true;
        }
        return false;
    }

    private static void appendString(StringBuilder buff, PropertyValue p) {
        if (p.isArray()) {
            if (p.getType() == Type.BINARIES) {
                // OAK-2050: don't try to load binaries as this would 
                // run out of memory
            } else {
                for (String v : p.getValue(STRINGS)) {
                    buff.append(v).append(' ');
                }
            }
        } else {
            if (p.getType() == Type.BINARY) {
                // OAK-2050: don't try to load binaries as this would 
                // run out of memory
            } else {
                buff.append(p.getValue(STRING)).append(' ');
            }
        }
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public void restrict(FilterImpl f) {
        f.restrictFulltextCondition(fullTextSearchExpression.currentValue().getValue(Type.STRING));
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        if (s.equals(selector)) {
            selector.restrictSelector(this);
        }
    }

    /**
     * restrict the provided property to the property to the provided filter achieving so
     * {@code property IS NOT NULL}
     * 
     * @param propertyName
     * @param f
     */
    void restrictPropertyOnFilter(String propertyName, FilterImpl f) {
        f.restrictProperty(propertyName, Operator.NOT_EQUAL, null);
    }

    @Override
    public AstElement copyOf() {
        return new FullTextSearchImpl(selectorName, propertyName, fullTextSearchExpression);
    }

    @Override
    public boolean requiresFullTextIndex() {
        return true;
    }

}
