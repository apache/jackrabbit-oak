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
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class XPathQueryBuilder implements QueryBuilder<Condition> {

    private AuthorizableType selectorType = AuthorizableType.AUTHORIZABLE;
    private String groupID;
    private boolean declaredMembersOnly;
    private Condition condition;
    private String sortProperty;
    private Direction sortDirection = Direction.ASCENDING;
    private boolean sortIgnoreCase;
    private Value bound;
    private long offset;
    private long maxCount = Long.MAX_VALUE;

    //-------------------------------------------------------< QueryBuilder >---
    @Override
    public void setSelector(@NotNull Class<? extends Authorizable> selector) {
        if (User.class.isAssignableFrom(selector)) {
            selectorType = AuthorizableType.USER;
        } else if (Group.class.isAssignableFrom(selector)) {
            selectorType = AuthorizableType.GROUP;
        } else {
            selectorType = AuthorizableType.AUTHORIZABLE;
        }
    }

    @Override
    public void setScope(@NotNull String groupID, boolean declaredOnly) {
        this.groupID = groupID;
        declaredMembersOnly = declaredOnly;
    }

    @Override
    public void setCondition(@NotNull Condition condition) {
        this.condition = condition;
    }

    @Override
    public void setSortOrder(@NotNull String propertyName, @NotNull Direction direction, boolean ignoreCase) {
        sortProperty = propertyName;
        sortDirection = direction;
        sortIgnoreCase = ignoreCase;
    }

    @Override
    public void setSortOrder(@NotNull String propertyName, @NotNull Direction direction) {
        setSortOrder(propertyName, direction, false);
    }

    @Override
    public void setLimit(@Nullable Value bound, long maxCount) {
        // reset the offset before setting bound value/maxCount
        offset = 0;
        this.bound = bound;
        setMaxCount(maxCount);
    }

    @Override
    public void setLimit(long offset, long maxCount) {
        // reset the bound value before setting offset/maxCount
        bound = null;
        this.offset = offset;
        setMaxCount(maxCount);
    }

    @NotNull
    @Override
    public Condition nameMatches(@NotNull String pattern) {
        return new Condition.Node(pattern);
    }

    @NotNull
    @Override
    public Condition neq(@NotNull String relPath, @NotNull Value value) {
        return new Condition.PropertyValue(relPath, RelationOp.NE, value);
    }

    @NotNull
    @Override
    public Condition eq(@NotNull String relPath, @NotNull Value value) {
        return new Condition.PropertyValue(relPath, RelationOp.EQ, value);
    }

    @NotNull
    @Override
    public Condition lt(@NotNull String relPath, @NotNull Value value) {
        return new Condition.PropertyValue(relPath, RelationOp.LT, value);
    }

    @NotNull
    @Override
    public Condition le(@NotNull String relPath, @NotNull Value value) {
        return new Condition.PropertyValue(relPath, RelationOp.LE, value);
    }

    @NotNull
    @Override
    public Condition gt(@NotNull String relPath, @NotNull Value value) {
        return new Condition.PropertyValue(relPath, RelationOp.GT, value);
    }

    @NotNull
    @Override
    public Condition ge(@NotNull String relPath, @NotNull Value value) {
        return new Condition.PropertyValue(relPath, RelationOp.GE, value);
    }

    @NotNull
    @Override
    public Condition exists(@NotNull String relPath) {
        return new Condition.PropertyExists(relPath);
    }

    @NotNull
    @Override
    public Condition like(@NotNull String relPath, @NotNull String pattern) {
        return new Condition.PropertyLike(relPath, pattern);
    }

    @NotNull
    @Override
    public Condition contains(@NotNull String relPath, @NotNull String searchExpr) {
        return new Condition.Contains(relPath, searchExpr);
    }

    @NotNull
    @Override
    public Condition impersonates(@NotNull String name) {
        return new Condition.Impersonation(name);
    }

    @NotNull
    @Override
    public Condition not(@NotNull Condition condition) {
        return new Condition.Not(condition);
    }

    @NotNull
    @Override
    public Condition and(@NotNull Condition condition1, @NotNull Condition condition2) {
        return new Condition.And(condition1, condition2);
    }

    @NotNull
    @Override
    public Condition or(@NotNull Condition condition1, @NotNull Condition condition2) {
        return new Condition.Or(condition1, condition2);
    }

    //-----------------------------------------------------------< internal >---
    Condition property(@NotNull String relPath, @NotNull RelationOp op, @NotNull Value value) {
        return new Condition.PropertyValue(relPath, op, value);
    }

    AuthorizableType getSelectorType() {
        return selectorType;
    }

    String getGroupID() {
        return groupID;
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

    private void setMaxCount(long maxCount) {
        if (maxCount == -1) {
            this.maxCount = Long.MAX_VALUE;
        } else {
            this.maxCount = maxCount;
        }
    }
}