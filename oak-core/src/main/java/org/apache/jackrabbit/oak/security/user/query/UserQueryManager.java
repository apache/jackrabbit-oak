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

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;

import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.user.UserManagerImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.ISO9075;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query manager for user specific searches.
 */
public class UserQueryManager {

    private static final Logger log = LoggerFactory.getLogger(UserQueryManager.class);

    private final UserManagerImpl userManager;
    private final NamePathMapper namePathMapper;
    private final ConfigurationParameters config;
    private final Root root;

    public UserQueryManager(@Nonnull UserManagerImpl userManager,
                            @Nonnull NamePathMapper namePathMapper,
                            @Nonnull ConfigurationParameters config,
                            @Nonnull Root root) {
        this.userManager = userManager;
        this.namePathMapper = namePathMapper;
        this.config = config;
        this.root = root;
    }

    /**
     * Find the authorizables matching the specified user {@code Query}.
     *
     * @param query A query object.
     * @return An iterator of authorizables that match the specified query.
     * @throws RepositoryException If an error occurs.
     */
    @Nonnull
    public Iterator<Authorizable> findAuthorizables(@Nonnull Query query) throws RepositoryException {
        XPathQueryBuilder builder = new XPathQueryBuilder();
        query.build(builder);

        if (builder.getMaxCount() == 0) {
            return Iterators.emptyIterator();
        }

        String statement = buildXPathStatement(builder);
        final String groupId = builder.getGroupID();
        if (groupId == null || isEveryone(groupId)) {
            long offset = builder.getOffset();
            Iterator<Authorizable> result = findAuthorizables(statement, builder.getMaxCount(), offset, null);
            if (groupId == null) {
                return result;
            } else {
                return Iterators.filter(result, authorizable -> {
                    try {
                        return authorizable != null && !groupId.equals(authorizable.getID());
                    } catch (RepositoryException e) {
                        return false;
                    }
                });
            }
        } else {
            // filtering by group name included in query -> enforce offset and limit on the result set.
            Iterator<Authorizable> result = findAuthorizables(statement, Long.MAX_VALUE, 0, null);
            Predicate<Authorizable> groupFilter = new GroupPredicate(userManager, groupId, builder.isDeclaredMembersOnly());
            return ResultIterator.create(builder.getOffset(), builder.getMaxCount(),
                    Iterators.filter(result, groupFilter));
        }
    }

    /**
     * Find the authorizables matching the following search parameters within
     * the sub-tree defined by an authorizable tree:
     *
     * @param relPath          A relative path (or a name) pointing to properties within
     *                         the tree defined by a given authorizable node.
     * @param value            The property value to look for.
     * @param authorizableType Filter the search results to only return authorizable
     *                         trees of a given type. Passing {@link org.apache.jackrabbit.oak.spi.security.user.AuthorizableType#AUTHORIZABLE} indicates that
     *                         no filtering for a specific authorizable type is desired. However, properties
     *                         might still be search in the complete sub-tree of authorizables depending
     *                         on the other query parameters.
     * @return An iterator of authorizable trees that match the specified
     *         search parameters and filters or an empty iterator if no result can be
     *         found.
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    @Nonnull
    public Iterator<Authorizable> findAuthorizables(@Nonnull String relPath,
                                                    @Nullable String value,
                                                    @Nonnull AuthorizableType authorizableType) throws RepositoryException {
        return findAuthorizables(relPath, value, authorizableType, true);
    }

    /**
     * Find the authorizables matching the following search parameters within
     * the sub-tree defined by an authorizable tree:
     *
     * @param relPath          A relative path (or a name) pointing to properties within
     *                         the tree defined by a given authorizable node.
     * @param value            The property value to look for.
     * @param authorizableType Filter the search results to only return authorizable
     *                         trees of a given type. Passing {@link org.apache.jackrabbit.oak.spi.security.user.AuthorizableType#AUTHORIZABLE} indicates that
     *                         no filtering for a specific authorizable type is desired. However, properties
     *                         might still be search in the complete sub-tree of authorizables depending
     *                         on the other query parameters.
     * @param exact            A boolean flag indicating if the value must match exactly or not.s
     * @return An iterator of authorizable trees that match the specified
     *         search parameters and filters or an empty iterator if no result can be
     *         found.
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    @Nonnull
    public Iterator<Authorizable> findAuthorizables(@Nonnull String relPath,
                                                    @Nullable String value,
                                                    @Nonnull AuthorizableType authorizableType,
                                                    boolean exact) throws RepositoryException {
        String statement = buildXPathStatement(relPath, value, authorizableType, exact);
        return findAuthorizables(statement, Long.MAX_VALUE, 0, authorizableType);
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private String buildXPathStatement(@Nonnull String relPath,
                                       @Nullable String value,
                                       @Nonnull AuthorizableType type, boolean exact) {
        StringBuilder stmt = new StringBuilder();
        String searchRoot = namePathMapper.getJcrPath(QueryUtil.getSearchRoot(type, config));
        if (!"/".equals(searchRoot)) {
            stmt.append(searchRoot);
        }

        String propName = Text.getName(relPath);
        String path;
        String ntName;
        if (relPath.indexOf('/') == -1 && !isReserved(propName)) {
            // arbitrary property specified in query and no explicit relative path specified
            // -> need to search within the whole in the authorizable tree
            path = null;
            ntName = null;
        } else {
            path = getQueryPath(relPath);
            ntName = (path == null) ? namePathMapper.getJcrName(QueryUtil.getNodeTypeName(type)) : null;
        }

        stmt.append("//");
        if (path != null) {
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
    private String buildXPathStatement(@Nonnull XPathQueryBuilder builder) throws RepositoryException {
        Condition condition = builder.getCondition();
        String sortCol = builder.getSortProperty();
        QueryBuilder.Direction sortDir = builder.getSortDirection();
        Value bound = builder.getBound();

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
 
        String searchRoot = namePathMapper.getJcrPath(QueryUtil.getSearchRoot(builder.getSelectorType(), config));
        String ntName = namePathMapper.getJcrName(QueryUtil.getNodeTypeName(builder.getSelectorType()));
        statement.append(searchRoot).append("//element(*,").append(ntName).append(')');

        if (condition != null) {
            ConditionVisitor visitor = new XPathConditionVisitor(statement, namePathMapper, userManager);
            statement.append('[');
            condition.accept(visitor);
            statement.append(']');
        }

        if (sortCol != null) {
            boolean ignoreCase = builder.getSortIgnoreCase();
            statement.append(" order by ");
            if (ignoreCase) {
                statement.append("fn:lower-case(").append(sortCol).append(')');
            } else {
                statement.append(sortCol);
            }
            statement.append(' ').append(sortDir.getDirection());
        }

        return statement.toString();
    }

    @Nonnull
    private Iterator<Authorizable> findAuthorizables(@Nonnull String statement,
                                                     long limit,
                                                     long offset,
                                                     @Nullable AuthorizableType type) throws RepositoryException {
        try {
            Result query = root.getQueryEngine().executeQuery(
                    statement, javax.jcr.query.Query.XPATH, limit, offset,
                    NO_BINDINGS, namePathMapper.getSessionLocalMappings());
            Iterable<? extends ResultRow> resultRows = query.getRows();
            Iterator<Authorizable> authorizables = Iterators.transform(resultRows.iterator(), new ResultRowToAuthorizable(userManager, root, type));
            return Iterators.filter(authorizables, new UniqueResultPredicate());
        } catch (ParseException e) {
            log.warn("Invalid user query: " + statement, e);
            throw new RepositoryException(e);
        }
    }

    @CheckForNull
    private static String getQueryPath(@Nonnull String relPath) {
        if (relPath.indexOf('/') == -1) {
            // just a single segment -> don't include the path in the query
            return null;
        } else {
            // compute the relative path excluding the trailing property name
            String[] segments = Text.explode(relPath, '/', false);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < segments.length - 1; i++) {
                if (!PathUtils.denotesCurrent(segments[i])) {
                    if (i > 0) {
                        sb.append('/');
                    }
                    sb.append(segments[i]);
                }
            }
            return Strings.emptyToNull(sb.toString());
        }
    }

    @CheckForNull
    private static boolean isReserved(@Nonnull String propName) {
        return UserConstants.GROUP_PROPERTY_NAMES.contains(propName) || UserConstants.USER_PROPERTY_NAMES.contains(propName);
    }

    private boolean isEveryone(@Nonnull String groupId) throws RepositoryException {
        Group gr = userManager.getAuthorizable(groupId, Group.class);
        if (gr == null) {
            // compatibility with original code that didn't check for existence of the group
            return EveryonePrincipal.NAME.equals(groupId);
        } else {
            return EveryonePrincipal.NAME.equals(gr.getPrincipal().getName());
        }
    }

    /**
     * Predicate asserting that a given user/group is only included once in the
     * result set.
     */
    private static final class UniqueResultPredicate implements Predicate<Authorizable> {

        private final Set<String> authorizableIds = new HashSet();

        @Override
        public boolean apply(@Nullable Authorizable input) {
            try {
                if (input != null) {
                    return authorizableIds.add(input.getID());
                }
            } catch (RepositoryException e) {
                log.debug("Failed to retrieve authorizable ID " + e.getMessage());
            }
            return false;
        }
    }
}
