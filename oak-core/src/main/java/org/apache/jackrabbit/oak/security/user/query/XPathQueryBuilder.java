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
package org.apache.jackrabbit.oak.security.user.query;

import javax.jcr.Value;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.QueryBuilder;

public class XPathQueryBuilder implements QueryBuilder<Condition> {

    private Class<? extends Authorizable> selector = Authorizable.class;
    private String groupName;
    private boolean declaredMembersOnly;
    private Condition condition;
    private String sortProperty;
    private Direction sortDirection = Direction.ASCENDING;
    private boolean sortIgnoreCase;
    private Value bound;
    private long offset;
    private long maxCount = -1;

    //-------------------------------------------------------< QueryBuilder >---
    @Override
    public void setSelector(Class<? extends Authorizable> selector) {
        this.selector = selector;
    }

    @Override
    public void setScope(String groupName, boolean declaredOnly) {
        this.groupName = groupName;
        declaredMembersOnly = declaredOnly;
    }

    @Override
    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    @Override
    public void setSortOrder(String propertyName, Direction direction, boolean ignoreCase) {
        sortProperty = propertyName;
        sortDirection = direction;
        sortIgnoreCase = ignoreCase;
    }

    @Override
    public void setSortOrder(String propertyName, Direction direction) {
        setSortOrder(propertyName, direction, false);
    }

    @Override
    public void setLimit(Value bound, long maxCount) {
        offset = 0;   // Unset any previously set offset
        this.bound = bound;
        this.maxCount = maxCount;
    }

    @Override
    public void setLimit(long offset, long maxCount) {
        bound = null; // Unset any previously set bound
        this.offset = offset;
        this.maxCount = maxCount;
    }

    @Override
    public Condition nameMatches(String pattern) {
        return new Condition.Node(pattern);
    }

    @Override
    public Condition neq(String relPath, Value value) {
        return new Condition.Property(relPath, RelationOp.NE, value);
    }

    @Override
    public Condition eq(String relPath, Value value) {
        return new Condition.Property(relPath, RelationOp.EQ, value);
    }

    @Override
    public Condition lt(String relPath, Value value) {
        return new Condition.Property(relPath, RelationOp.LT, value);
    }

    @Override
    public Condition le(String relPath, Value value) {
        return new Condition.Property(relPath, RelationOp.LE, value);
    }

    @Override
    public Condition gt(String relPath, Value value) {
        return new Condition.Property(relPath, RelationOp.GT, value);
    }

    @Override
    public Condition ge(String relPath, Value value) {
        return new Condition.Property(relPath, RelationOp.GE, value);
    }

    @Override
    public Condition exists(String relPath) {
        return new Condition.Property(relPath, RelationOp.EX);
    }

    @Override
    public Condition like(String relPath, String pattern) {
        return new Condition.Property(relPath, RelationOp.LIKE, pattern);
    }

    @Override
    public Condition contains(String relPath, String searchExpr) {
        return new Condition.Contains(relPath, searchExpr);
    }

    @Override
    public Condition impersonates(String name) {
        return new Condition.Impersonation(name);
    }

    @Override
    public Condition not(Condition condition) {
        return new Condition.Not(condition);
    }

    @Override
    public Condition and(Condition condition1, Condition condition2) {
        return new Condition.And(condition1, condition2);
    }

    @Override
    public Condition or(Condition condition1, Condition condition2) {
        return new Condition.Or(condition1, condition2);
    }

    //-----------------------------------------------------------< internal >---

    Condition property(String relPath, RelationOp op, Value value) {
        return new Condition.Property(relPath, op, value);
    }

    Class<? extends Authorizable> getSelector() {
        return selector;
    }

    String getGroupName() {
        return groupName;
    }

    boolean isDeclaredMembersOnly() {
        return declaredMembersOnly;
    }

    Condition getCondition() {
        return condition;
    }

    String getSortProperty() {
        return sortProperty;
    }

    Direction getSortDirection() {
        return sortDirection;
    }

    boolean getSortIgnoreCase() {
        return sortIgnoreCase;
    }

    Value getBound() {
        return bound;
    }

    long getOffset() {
        return offset;
    }

    long getMaxCount() {
        return maxCount;
    }
}