/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr.query.qom;

import javax.jcr.query.qom.Join;

/**
 * The implementation of the corresponding JCR interface.
 */
public class JoinImpl extends SourceImpl implements Join {

    private final JoinConditionImpl joinCondition;
    private final JoinType joinType;
    private final SourceImpl left;
    private final SourceImpl right;

    public JoinImpl(SourceImpl left, SourceImpl right, JoinType joinType,
            JoinConditionImpl joinCondition) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.joinCondition = joinCondition;
    }

    @Override
    public JoinConditionImpl getJoinCondition() {
        return joinCondition;
    }

    @Override
    public String getJoinType() {
        return joinType.toString();
    }

    @Override
    public SourceImpl getLeft() {
        return left;
    }

    @Override
    public SourceImpl getRight() {
        return right;
    }

    @Override
    public String toString() {
        return joinType.formatSql(left, right, joinCondition);
    }

}
