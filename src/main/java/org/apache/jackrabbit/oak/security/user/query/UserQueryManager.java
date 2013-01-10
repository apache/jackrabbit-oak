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

import java.text.ParseException;
import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.util.ISO9075;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserQueryManager... TODO
 *
 * TODO OAK-253: replace usage of XPATH
 */
public class UserQueryManager {

    private static final Logger log = LoggerFactory.getLogger(UserQueryManager.class);

    private final UserManager userManager;
    private final NamePathMapper namePathMapper;
    private final ConfigurationParameters config;
    private final QueryEngine queryEngine;

    public UserQueryManager(UserManager userManager, NamePathMapper namePathMapper,
                            ConfigurationParameters config, QueryEngine queryEngine) {
        this.userManager = userManager;
        this.namePathMapper = namePathMapper;
        this.config = config;
        this.queryEngine = queryEngine;
    }

    /**
     * Find the authorizables matching the specified user {@code Query}.
     *
     * @param query A query object.
     * @return An iterator of authorizables that match the specified query.
     * @throws RepositoryException If an error occurs.
     */
    @Nonnull
    public Iterator<Authorizable> findAuthorizables(Query query) throws RepositoryException {
        XPathQueryBuilder builder = new XPathQueryBuilder();
        query.build(builder);

        if (builder.getMaxCount() == 0) {
            return Iterators.emptyIterator();
        }

        Value bound = builder.getBound();
        Condition condition = builder.getCondition();
        String sortCol = builder.getSortProperty();
        QueryBuilder.Direction sortDir = builder.getSortDirection();
        if (bound != null) {
            if (sortCol == null) {
                log.warn("Ignoring bound {} since no sort order is specified");
            } else {
                Condition boundCondition = builder.property(sortCol, QueryUtil.getCollation(sortDir), bound);
                if (condition == null) {
                    condition = boundCondition;
                } else {
                    condition = builder.and(condition, boundCondition);
                }
            }
        }

        StringBuilder statement = new StringBuilder();
        ConditionVisitor visitor = new XPathConditionVisitor(statement, namePathMapper);

        String searchRoot = QueryUtil.getSearchRoot(builder.getSelectorType(), config);
        String ntName = QueryUtil.getNodeTypeName(builder.getSelectorType());
        statement.append(searchRoot).append("//element(*,").append(ntName).append(')');

        if (condition != null) {
            statement.append('[');
            condition.accept(visitor);
            statement.append(']');
        }

        if (sortCol != null) {
            boolean ignoreCase = builder.getSortIgnoreCase();
            statement.append(" order by ")
                    .append(ignoreCase ? "" : "fn:lower-case(")
                    .append(sortCol)
                    .append(ignoreCase ? " " : ") ")
                    .append(sortDir.getDirection());
        }

        if (builder.getGroupName() == null) {
            long offset = builder.getOffset();
            if (bound != null && offset > 0) {
                log.warn("Found bound {} and offset {} in limit. Discarding offset.", bound, offset);
                offset = 0;
            }
            return findAuthorizables(statement.toString(), builder.getMaxCount(), offset);
        } else {
            // filtering by group name included in query -> enforce offset
            // and limit on the result set.
            Iterator<Authorizable> result = findAuthorizables(statement.toString(), Long.MAX_VALUE, 0);
            Predicate groupFilter = new GroupPredicate(userManager,
                    builder.getGroupName(),
                    builder.isDeclaredMembersOnly());
            return ResultIterator.create(builder.getOffset(), builder.getMaxCount(),
                    Iterators.filter(result, groupFilter));
        }
    }

    /**
     * Find the authorizables matching the following search parameters within
     * the sub-tree defined by an authorizable tree:
     *
     * @param relPath A relative path (or a name) pointing to properties within
     * the tree defined by a given authorizable node.
     * @param value The property value to look for.
     * @param authorizableType Filter the search results to only return authorizable
     * trees of a given type. Passing {@link org.apache.jackrabbit.oak.spi.security.user.AuthorizableType#AUTHORIZABLE} indicates that
     * no filtering for a specific authorizable type is desired. However, properties
     * might still be search in the complete sub-tree of authorizables depending
     * on the other query parameters.
     * @return An iterator of authorizable trees that match the specified
     * search parameters and filters or an empty iterator if no result can be
     * found.
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    @Nonnull
    public Iterator<Authorizable> findAuthorizables(String relPath, String value,
                                                    AuthorizableType authorizableType) throws RepositoryException {
        return findAuthorizables(relPath, value, authorizableType, true);
    }

    /**
     * Find the authorizables matching the following search parameters within
     * the sub-tree defined by an authorizable tree:
     *
     *
     * @param relPath A relative path (or a name) pointing to properties within
     * the tree defined by a given authorizable node.
     * @param value The property value to look for.
     * @param authorizableType Filter the search results to only return authorizable
     * trees of a given type. Passing {@link org.apache.jackrabbit.oak.spi.security.user.AuthorizableType#AUTHORIZABLE} indicates that
     * no filtering for a specific authorizable type is desired. However, properties
     * might still be search in the complete sub-tree of authorizables depending
     * on the other query parameters.
     * @param exact A boolean flag indicating if the value must match exactly or not.s
     * @return An iterator of authorizable trees that match the specified
     * search parameters and filters or an empty iterator if no result can be
     * found.
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    @Nonnull
    public Iterator<Authorizable> findAuthorizables(String relPath, String value,
                                                    AuthorizableType authorizableType, boolean exact) throws RepositoryException {
        String statement = buildXPathStatement(relPath, value, authorizableType, exact);
        return findAuthorizables(statement, Long.MAX_VALUE, 0);
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private String buildXPathStatement(String relPath, String value, AuthorizableType type, boolean exact) {
        StringBuilder stmt = new StringBuilder();
        String searchRoot = QueryUtil.getSearchRoot(type, config);
        if (!"/".equals(searchRoot)) {
            stmt.append(searchRoot);
        }

        String propName;
        String path;
        String ntName;
        if (relPath.indexOf('/') == -1) {
            // search for properties somewhere below an authorizable node
            propName = namePathMapper.getOakName(relPath);
            path = null;
            ntName = null;
        } else {
            // FIXME: proper normalization of the relative path
            String oakPath = namePathMapper.getOakPath(relPath);
            propName = Text.getName(oakPath);
            path = Text.getRelativeParent(oakPath, 1);
            ntName = QueryUtil.getNodeTypeName(type);
        }

        stmt.append("//");
        if (path != null && !path.isEmpty()) {
            stmt.append(path);
        } else if (ntName != null) {
            stmt.append("element(*,").append(ntName).append(')');
        } else {
            stmt.append("element(*)");
        }

        if (value == null) {
            // property must exist
            stmt.append("[@").append(propName).append(']');
        } else {
            stmt.append('[');
            stmt.append((exact) ? "@" : "jcr:like(@");
            stmt.append(ISO9075.encode(propName));
            if (exact) {
                stmt.append("='");
                stmt.append(value.replaceAll("'", "''"));
                stmt.append('\'');
            } else {
                stmt.append(",'%");
                stmt.append(QueryUtil.escapeForQuery(value));
                stmt.append("%')");
            }
            stmt.append(']');
        }
        return stmt.toString();
    }

    @Nonnull
    private Iterator<Authorizable> findAuthorizables(String statement, long limit,
                                                    long offset) throws RepositoryException {
        try {
            Iterable<? extends ResultRow> resultRows = queryEngine.executeQuery(statement, javax.jcr.query.Query.XPATH, limit, offset, null, namePathMapper).getRows();
            Iterator<Authorizable> authorizables = Iterators.transform(resultRows.iterator(), new ResultRowToAuthorizable(userManager));
            return Iterators.filter(authorizables, Predicates.<Object>notNull());
        } catch (ParseException e) {
            log.warn("Invalid user query: " + statement, e);
            throw new RepositoryException(e);
        }
    }
}