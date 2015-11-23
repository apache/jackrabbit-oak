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
package org.apache.jackrabbit.oak.query.ast;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

public class NotFullTextSearchImpl extends FullTextSearchImpl {
    private static final Set<String> KEYWORDS = ImmutableSet.of("or");
    private static final Splitter SPACE_SPLITTER = Splitter.on(' ').omitEmptyStrings().trimResults();

    public NotFullTextSearchImpl(String selectorName, String propertyName,
                                 StaticOperandImpl fullTextSearchExpression) {
        super(selectorName, propertyName, fullTextSearchExpression);
    }

    public NotFullTextSearchImpl(FullTextSearchImpl ft) {
        this(ft.selectorName, ft.propertyName, ft.fullTextSearchExpression);
    }

    @Override
    ConstraintImpl not() {
        return new FullTextSearchImpl(this.selectorName, this.propertyName,
                this.fullTextSearchExpression);
    }

    @Override
    String getRawText(PropertyValue v) {
        Iterable<String> terms = SPACE_SPLITTER.split(super.getRawText(v));
        StringBuffer raw = new StringBuffer();
        for (String term : terms) {
            if (isKeyword(term)) {
                raw.append(String.format("%s ", term));
            } else {
                raw.append(String.format("-%s ", term));
            }
        }
        return raw.toString().trim();
    }

    private static boolean isKeyword(@Nonnull String term) {
        return KEYWORDS.contains(checkNotNull(term).toLowerCase());
    }
    
    @Override
    void restrictPropertyOnFilter(String propertyName, FilterImpl f) {
        // Intentionally left empty. A NOT CONTAINS() can be valid if the property is actually not
        // there.
    }

    @Override
    public String toString() {
        return "not " + super.toString();
    }

    @Override
    boolean enforcePropertyExistence(String propertyName, SelectorImpl selector) {
        // in case of NOT CONTAINS we want to match nodes without the property as well. In this way
        // we don't care whether the property is there or not.
        return true;
    }

    @Override
    public boolean requiresFullTextIndex() {
        return true;
    }

}
