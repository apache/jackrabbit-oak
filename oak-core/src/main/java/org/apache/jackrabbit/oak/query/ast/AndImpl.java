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
 * An AND condition.
 */
public class AndImpl extends ConstraintImpl {

    private final ConstraintImpl constraint1, constraint2;

    public AndImpl(ConstraintImpl constraint1, ConstraintImpl constraint2) {
        this.constraint1 = constraint1;
        this.constraint2 = constraint2;
    }

    public ConstraintImpl getConstraint1() {
        return constraint1;
    }

    public ConstraintImpl getConstraint2() {
        return constraint2;
    }

    @Override
    public boolean evaluate() {
        return constraint1.evaluate() && constraint2.evaluate();
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return protect(constraint1) + " and " + protect(constraint2);
    }

    @Override
    public void restrict(FilterImpl f) {
        constraint1.restrict(f);
        constraint2.restrict(f);
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        constraint1.restrictPushDown(s);
        constraint2.restrictPushDown(s);
    }

}

