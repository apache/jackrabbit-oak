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
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.query.QueryIndex.NativeQueryIndex;

/**
 * A native function condition.
 */
public class NativeFunctionImpl extends ConstraintImpl {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * The prefix for native function restrictions (native, similar, spellcheck, suggest).
     */
    public static final String NATIVE_PREFIX = "native*";

    private final String selectorName;
    private final String language;
    private final StaticOperandImpl nativeSearchExpression;
    private SelectorImpl selector;
    
    NativeFunctionImpl(String selectorName, String language, StaticOperandImpl expression) {
        this.selectorName = selectorName;
        this.language = language;
        this.nativeSearchExpression = expression;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("native(");
        builder.append(quote(selectorName));
        builder.append(", ");
        builder.append(quote(language));
        builder.append(", ");
        builder.append(getNativeSearchExpression());
        builder.append(')');
        return builder.toString();
    }
    
    @Override
    public boolean evaluate() {
        // disable evaluation if a fulltext index is used,
        // and because we don't know how to process native
        // conditions
        if (!(selector.getIndex() instanceof NativeQueryIndex)) {
            log.warn("No full-text index was found that can process the condition " + toString());
            return false;
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
            PropertyValue v = nativeSearchExpression.currentValue();
            f.restrictProperty(NATIVE_PREFIX + language, Operator.EQUAL, v);
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
    
    public StaticOperandImpl getNativeSearchExpression() {
        return nativeSearchExpression;
    }

    @Override
    public AstElement copyOf() {
        return new NativeFunctionImpl(selectorName, language, nativeSearchExpression);
    }

    @Override
    public boolean requiresFullTextIndex() {
        return true;
    }

}
