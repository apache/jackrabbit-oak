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

package org.apache.jackrabbit.api.security.user;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.Value;

public interface QueryBuilder<T> {

    /**
     * The sort order of the result set of a query.
     */
    enum Direction {
        ASCENDING("ascending"),
        DESCENDING("descending");

        private final String direction;

        Direction(String direction) {
            this.direction = direction;
        }

        public String getDirection() {
            return direction;
        }
    }

    /**
     * Set the selector for the query. The selector determines whether the query
     * returns all {@link Authorizable}s or just {@link User}s respectively {@link Group}s. 
     *
     * @param selector  The selector for the query
     */
    void setSelector(@NotNull Class<? extends Authorizable> selector);

    /**
     * Set the scope for the query. If set, the query will only return members of a specific group.
     *
     * @param groupName  Name of the group to restrict the query to.
     * @param declaredOnly  If <code>true</code> only declared members of the groups are returned.
     * Otherwise indirect memberships are also considered. 
     */
    void setScope(@NotNull String groupName, boolean declaredOnly);

    /**
     * Set the condition for the query. The query only includes {@link Authorizable}s
     * for which this condition holds.
     * 
     * @param condition  Condition upon which <code>Authorizables</code> are included in the query result
     */
    void setCondition(@NotNull T condition);

    /**
     * Set the sort order of the {@link Authorizable}s returned by the query.
     * The format of the <code>propertyName</code> is the same as in XPath:
     * <code>@propertyName</code> sorts on a property of the current node.
     * <code>relative/path/@propertyName</code> sorts on a property of a
     * descendant node.
     *
     * @param propertyName  The name of the property to sort on
     * @param direction  Direction to sort. Either {@link Direction#ASCENDING} or {@link Direction#DESCENDING}
     * @param ignoreCase  Ignore character case in sort if <code>true</code>. Note: For <code>false</code>
     * sorting is done lexicographically even for non string properties.
     */
    void setSortOrder(@NotNull String propertyName, @NotNull Direction direction, boolean ignoreCase);

    /**
     * Set the sort order of the {@link Authorizable}s returned by the query.
     * The format of the <code>propertyName</code> is the same as in XPath:
     * <code>@propertyName</code> sorts on a property of the current node.
     * <code>relative/path/@propertyName</code> sorts on a property of a
     * descendant node. Character case is taken into account for the sort order.
     *
     * @param propertyName  The name of the property to sort on
     * @param direction  Direction to sort. Either {@link Direction#ASCENDING} or {@link Direction#DESCENDING}
     */
    void setSortOrder(@NotNull String propertyName, @NotNull Direction direction);

    /**
     * Set limits for the query. The limits consists of a bound and a maximal
     * number of results. The bound refers to the value of the
     * {@link #setSortOrder(String, Direction) sort order} property. The
     * query returns at most <code>maxCount</code> {@link Authorizable}s whose
     * values of the sort order property follow <code>bound</code> in the sort
     * direction. This method has no effect if the sort order is not specified.
     *
     * @param bound  Bound from where to start returning results. <code>null</code>
     * for no bound
     * @param maxCount  Maximal number of results to return. -1 for no limit.
     */
    void setLimit(@Nullable Value bound, long maxCount);

    /**
     * Set limits for the query. The limits consists of an offset and a maximal
     * number of results. <code>offset</code> refers to the offset within the full
     * result set at which the returned result set should start expressed in terms 
     * of the number of {@link Authorizable}s to skip. <code>maxCount</code> sets the
     * maximum size of the result set expressed in terms of the number of authorizables
     * to return.
     *
     * @param offset  Offset from where to start returning results. <code>0</code> for no offset.
     * @param maxCount  Maximal number of results to return. -1 for no limit.
     */
    void setLimit(long offset, long maxCount);

    /**
     * Create a condition which holds if the name of the {@link Authorizable}
     * matches a <code>pattern</code>.
     * The percent character "%" represents any string of zero or more characters and the
     * underscore character "_" represents any single character. Any literal use of these characters
     * and the backslash character "\" must be escaped with a backslash character.
     * The pattern is matched against the {@link Authorizable#getID() id} and the
     * {@link Authorizable#getPrincipal() principal}.
     *
     * @param pattern Pattern to match the name of an authorizable.
     * @return  A condition
     */
    @NotNull
    T nameMatches(@NotNull String pattern);

    /**
     * Create a condition which holds if the node of an {@link Authorizable} has a
     * property at <code>relPath</code> which is not equal to <code>value</code>.
     * The format of the <code>relPath</code> argument is the same as in XPath:
     * <code>@attributeName</code> for an attribute on this node and
     * <code>relative/path/@attributeName</code> for an attribute of a descendant node.
     *
     * @param relPath  Relative path from the authorizable's node to the property
     * @param value  Value to compare the property at <code>relPath</code> to
     * @return  A condition
     */
    @NotNull
    T neq(@NotNull String relPath, @NotNull Value value);

    /**
     * Create a condition which holds if the node of an {@link Authorizable} has a
     * property at <code>relPath</code> which is equal to <code>value</code>.
     * The format of the <code>relPath</code> argument is the same as in XPath:
     * <code>@attributeName</code> for an attribute on this node and
     * <code>relative/path/@attributeName</code> for an attribute of a descendant node.
     *
     * @param relPath  Relative path from the authorizable's node to the property
     * @param value  Value to compare the property at <code>relPath</code> to
     * @return  A condition
     */
    @NotNull
    T eq(@NotNull String relPath, @NotNull Value value);

    /**
     * Create a condition which holds if the node of an {@link Authorizable} has a
     * property at <code>relPath</code> which is smaller than <code>value</code>.
     * The format of the <code>relPath</code> argument is the same as in XPath:
     * <code>@attributeName</code> for an attribute on this node and
     * <code>relative/path/@attributeName</code> for an attribute of a descendant node.
     *
     * @param relPath  Relative path from the authorizable's node to the property
     * @param value  Value to compare the property at <code>relPath</code> to
     * @return  A condition
     */
    @NotNull
    T lt(@NotNull String relPath, @NotNull Value value);

    /**
     * Create a condition which holds if the node of an {@link Authorizable} has a
     * property at <code>relPath</code> which is smaller than or equal to <code>value</code>.
     * The format of the <code>relPath</code> argument is the same as in XPath:
     * <code>@attributeName</code> for an attribute on this node and
     * <code>relative/path/@attributeName</code> for an attribute of a descendant node.
     *
     * @param relPath  Relative path from the authorizable's node to the property
     * @param value  Value to compare the property at <code>relPath</code> to
     * @return  A condition
     */
    @NotNull
    T le(@NotNull String relPath, @NotNull Value value);

    /**
     * Create a condition which holds if the node of an {@link Authorizable} has a
     * property at <code>relPath</code> which is greater than <code>value</code>.
     * The format of the <code>relPath</code> argument is the same as in XPath:
     * <code>@attributeName</code> for an attribute on this node and
     * <code>relative/path/@attributeName</code> for an attribute of a descendant node.
     *
     * @param relPath  Relative path from the authorizable's node to the property
     * @param value  Value to compare the property at <code>relPath</code> to
     * @return  A condition
     */
    @NotNull
    T gt(@NotNull String relPath, @NotNull Value value);

    /**
     * Create a condition which holds if the node of an {@link Authorizable} has a
     * property at <code>relPath</code> which is greater than or equal to <code>value</code>.
     * The format of the <code>relPath</code> argument is the same as in XPath:
     * <code>@attributeName</code> for an attribute on this node and
     * <code>relative/path/@attributeName</code> for an attribute of a descendant node.
     *
     * @param relPath  Relative path from the authorizable's node to the property
     * @param value  Value to compare the property at <code>relPath</code> to
     * @return  A condition
     */
    @NotNull
    T ge(@NotNull String relPath, @NotNull Value value);

    /**
     * Create a condition which holds if the node of an {@link Authorizable} has a
     * property at <code>relPath</code>.
     * The format of the <code>relPath</code> argument is the same as in XPath:
     * <code>@attributeName</code> for an attribute on this node and
     * <code>relative/path/@attributeName</code> for an attribute of a descendant node.
     *
     * @param relPath  Relative path from the authorizable's node to the property
     * @return  A condition
     */
    @NotNull
    T exists(@NotNull String relPath);

    /**
     * Create a condition which holds if the node of an {@link Authorizable} has a
     * property at <code>relPath</code> which matches the pattern in <code>pattern</code>.
     * The percent character "%" represents any string of zero or more characters and the
     * underscore character "_" represents any single character. Any literal use of these characters
     * and the backslash character "\" must be escaped with a backslash character.
     * The format of the <code>relPath</code> argument is the same as in XPath:
     * <code>@attributeName</code> for an attribute on this node and
     * <code>relative/path/@attributeName</code> for an attribute of a descendant node.
     *
     * @param relPath  Relative path from the authorizable's node to the property
     * @param pattern  Pattern to match the property at <code>relPath</code> against
     * @return  A condition
     */
    @NotNull
    T like(@NotNull String relPath, @NotNull String pattern);

    /**
     * Create a full text search condition. The condition holds if the node of an
     * {@link Authorizable} has a property at <code>relPath</code> for which
     * <code>searchExpr</code> yields results.
     * The format of the <code>relPath</code> argument is the same as in XPath:
     * <code>.</code> searches all properties of the current node, <code>@attributeName</code>
     * searches the attributeName property of the current node, <code>relative/path/.</code>
     * searches all properties of the descendant node at relative/path and
     * <code>relative/path/@attributeName</code> searches the attributeName property
     * of the descendant node at relative/path.
     * The syntax of <code>searchExpr</code> is <pre>[-]value { [OR] [-]value }</pre>.
     *
     * @param relPath  Relative path from the authorizable's node to the property
     * @param searchExpr  A full text search expression
     * @return  A condition
     */
    @NotNull
    T contains(@NotNull String relPath, @NotNull String searchExpr);

    /**
     * Create a condition which holds for {@link Authorizable}s which can impersonate as
     * <code>name</code>.
     *
     * @param name  Name of an authorizable
     * @return  A condition
     */
    @NotNull
    T impersonates(@NotNull String name);

    /**
     * Return a condition which holds if <code>condition</code> does not hold.
     *
     * @param condition  Condition to negate
     * @return  A condition
     */
    @NotNull
    T not(@NotNull T condition);

    /**
     * Return a condition which holds if both sub conditions hold.
     *
     * @param condition1  first sub condition
     * @param condition2  second sub condition
     * @return  A condition
     */
    @NotNull
    T and(@NotNull T condition1, @NotNull T condition2);

    /**
     * Return a condition which holds if any of the two sub conditions hold.
     *
     * @param condition1  first sub condition
     * @param condition2  second sub condition
     * @return  A condition
     */
    @NotNull
    T or(@NotNull T condition1, @NotNull T condition2);
}
