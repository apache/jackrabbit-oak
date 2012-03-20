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
package org.apache.jackrabbit.oak.jcr.query.qom;

import javax.jcr.query.qom.Or;

/**
 * The implementation of the corresponding JCR interface.
 */
public class OrImpl extends ConstraintImpl implements Or {

    private final ConstraintImpl constraint1;
    private final ConstraintImpl constraint2;

    public OrImpl(ConstraintImpl constraint1, ConstraintImpl constraint2) {
        this.constraint1 = constraint1;
        this.constraint2 = constraint2;
    }

    @Override
    public ConstraintImpl getConstraint1() {
        return constraint1;
    }

    @Override
    public ConstraintImpl getConstraint2() {
        return constraint2;
    }

    @Override
    public String toString() {
        return protect(constraint1) + " OR " + protect(constraint2);
    }

    @Override
    public void bindVariables(QueryObjectModelImpl qom) {
        constraint1.bindVariables(qom);
        constraint2.bindVariables(qom);
    }

}
