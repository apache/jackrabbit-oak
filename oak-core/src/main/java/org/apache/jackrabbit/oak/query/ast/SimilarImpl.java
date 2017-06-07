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
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support for "similar(...)
 */
public class SimilarImpl extends ConstraintImpl {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public static final String NATIVE_LUCENE_LANGUAGE = "lucene";
    
    public static final String MORE_LIKE_THIS_PREFIX = "mlt?mlt.fl=:path&mlt.mindf=0&stream.body=";
    
    private final String selectorName;
    private final String propertyName;
    private final StaticOperandImpl pathExpression;
    private SelectorImpl selector;
    
    SimilarImpl(String selectorName, String propertyName, StaticOperandImpl pathExpression) {
        this.selectorName = selectorName;
        this.propertyName = propertyName;
        this.pathExpression = pathExpression;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("similar(");
        builder.append(quote(selectorName));
        if (propertyName != null) {
            builder.append(".").append(propertyName);
        }
        builder.append(", ");
        builder.append(getPathExpression());
        builder.append(')');
        return builder.toString();
    }
    
    @Override
    public boolean evaluate() {
        // disable evaluation if a fulltext index is used,
        // and because we don't know how to process native
        // conditions
        if (!(selector.getIndex() instanceof FulltextQueryIndex)) {
            log.warn("No full-text index was found that can process the condition " + toString());
            return false;
        }
        // verify the path is readable
        PropertyValue p = pathExpression.currentValue();
        String path = p.getValue(Type.STRING);
        if (selector.getTree(path) == null) {
            return false;
        }
        if (propertyName != null) {
            if (selector.currentProperty(propertyName) == null) {
                // property not found
                return false;
            }
        }
        
        // we assume the index only returns the requested entries
        return true;
    }

    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        return Collections.emptySet();
    }

    @Override
    public void restrict(FilterImpl f) {
        if (f.getSelector().equals(selector)) {
            PropertyValue p = pathExpression.currentValue();
            String path = p.getValue(Type.STRING);
            String query = MORE_LIKE_THIS_PREFIX + path;
            PropertyValue v = PropertyValues.newString(query);
            f.restrictProperty(NativeFunctionImpl.NATIVE_PREFIX + NATIVE_LUCENE_LANGUAGE, Operator.EQUAL, v);
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        if (s.equals(selector)) {
            selector.restrictSelector(this);
        }
    }

    @Override
    public Set<SelectorImpl> getSelectors() {
        return Collections.emptySet();
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }
    
    public StaticOperandImpl getPathExpression() {
        return pathExpression;
    }

    @Override
    public AstElement copyOf() {
        return new SimilarImpl(selectorName, propertyName, pathExpression);
    }

    @Override
    public boolean requiresFullTextIndex() {
        return true;
    }

}