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

import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * A "not" condition.
 */
public class NotImpl extends ConstraintImpl {

    private final ConstraintImpl constraint;

    public NotImpl(ConstraintImpl constraint) {
        this.constraint = constraint;
    }

    public ConstraintImpl getConstraint() {
        return constraint;
    }

    @Override
    public boolean evaluate() {
        return !constraint.evaluate();
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "not " + protect(constraint);
    }

    @Override
    public void restrict(FilterImpl f) {
        // ignore
        // TODO convert NOT conditions
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        // ignore
        // TODO convert NOT conditions
    }

}
