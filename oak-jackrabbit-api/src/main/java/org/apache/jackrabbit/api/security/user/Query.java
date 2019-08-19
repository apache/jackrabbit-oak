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

/**
 * A query to match {@link Authorizable}s. Pass an instance of this interface to
 * {@link UserManager#findAuthorizables(Query)}.
 *
 * The following query finds all users named 'Bob' which have the word
 * 'engineer' in its description and returns them in ascending order wrt. to
 * the name.
 *
 * <pre>
 *  Iterator&lt;Authorizable&gt; result = userMgr.findAuthorizables(new Query() {
 *      public &lt;T&gt; void build(QueryBuilder&lt;T&gt; builder) {
 *          builder.setCondition(builder.
 *              and(builder.
 *                  property("@name", RelationOp.EQ, valueFactory.createValue("Bob")), builder.
 *                  contains("@description", "engineer")));
 *
 *          builder.setSortOrder("@name", Direction.ASCENDING);
 *          builder.setSelector(Selector.USER);
 *      }
 *  });
 * </pre>
 */
public interface Query {

    /**
     * Build the query using a {@link QueryBuilder}.
     * @param builder  A query builder for building the query.
     * @param <T>  Opaque type of the query builder.
     */
    <T> void build(@NotNull QueryBuilder<T> builder);
}
