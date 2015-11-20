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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * class used to "wrap" a {@code NOT CONTAINS} clause. The main differences with a {@link NotImpl}
 * reside in the {@link NotImpl#evaluate()} and restricts.
 */
public class NotFullTextImpl extends NotImpl {
    public NotFullTextImpl(@Nonnull FullTextSearchImpl constraint) {
        super(new NotFullTextSearchImpl(checkNotNull(constraint)));
    }

    @Override
    public boolean evaluate() {
        return getConstraint().evaluate();
    }

    @Override
    public void restrict(FilterImpl f) {
        if (!f.getSelector().isOuterJoinRightHandSide()) {
            getConstraint().restrict(f);
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        getConstraint().restrictPushDown(s);    
    }
    
    @Override
    public FullTextExpression getFullTextConstraint(SelectorImpl s) {
        // TODO is this correct?
        ;
        return getConstraint().getFullTextConstraint(s);
    }

    @Override
    public boolean requiresFullTextIndex() {
        return true;
    }

}
