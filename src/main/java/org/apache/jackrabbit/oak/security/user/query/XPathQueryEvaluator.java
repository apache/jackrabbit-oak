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
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.query.Query;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This evaluator for {@link org.apache.jackrabbit.api.security.user.Query}s use XPath
 * and some minimal client side filtering.
 */
public class XPathQueryEvaluator implements ConditionVisitor {
    static final Logger log = LoggerFactory.getLogger(XPathQueryEvaluator.class);

    private final XPathQueryBuilder builder;
    private final UserManager userManager;
    private final Root root;
    private final NamePathMapper namePathMapper;

    private final StringBuilder xPath = new StringBuilder();

    public XPathQueryEvaluator(XPathQueryBuilder builder, UserManager userManager,
                               Root root, NamePathMapper namePathMapper) {
        this.builder = builder;
        this.userManager = userManager;
        this.root = root;
        this.namePathMapper = namePathMapper;
    }

    public Iterator<Authorizable> eval() throws RepositoryException {
        // shortcut
        if (builder.getMaxCount() == 0) {
            return Iterators.emptyIterator();
        }

        xPath.append("//element(*,")
                .append(getNtName(builder.getSelector()))
                .append(')');

        Value bound = builder.getBound();

        Condition condition = builder.getCondition();
        String sortCol = builder.getSortProperty();
        QueryBuilder.Direction sortDir = builder.getSortDirection();
        if (bound != null) {
            if (sortCol == null) {
                log.warn("Ignoring bound {} since no sort order is specified");
            } else {
                Condition boundCondition = builder.property(sortCol, getCollation(sortDir), bound);
                condition = condition == null
                        ? boundCondition
                        : builder.and(condition, boundCondition);
            }
        }

        if (condition != null) {
            xPath.append('[');
            condition.accept(this);
            xPath.append(']');
        }

        if (sortCol != null) {
            boolean ignoreCase = builder.getSortIgnoreCase();
            xPath.append(" order by ")
                    .append(ignoreCase ? "" : "fn:lower-case(")
                    .append(sortCol)
                    .append(ignoreCase ? " " : ") ")
                    .append(sortDir.getDirection());
        }

        try {
            if (builder.getGroupName() == null) {
                long offset = builder.getOffset();
                if (bound != null && offset > 0) {
                    log.warn("Found bound {} and offset {} in limit. Discarding offset.", bound, offset);
                    offset = 0;
                }
                return findAuthorizables(builder.getMaxCount(), offset);
            } else {
                // filtering by group name not included in query -> enforce offset
                // and limit on the result set.
                Iterator<Authorizable> result = findAuthorizables(Long.MAX_VALUE, 0);
                Iterator<Authorizable> filtered = filter(result, builder.getGroupName(), builder.isDeclaredMembersOnly());
                return ResultIterator.create(builder.getOffset(), builder.getMaxCount(), filtered);
            }
        } catch (ParseException e) {
            throw new RepositoryException(e);
        }

        // If we are scoped to a group and have a limit, we have to apply the limit
        // here (inefficient!) otherwise we can apply the limit in the query

    }

    //---------------------------------------------------< ConditionVisitor >---
    @Override
    public void visit(Condition.Node condition) throws RepositoryException {
        xPath.append('(')
                .append("jcr:like(@")
                .append(namePathMapper.getJcrName(UserConstants.REP_PRINCIPAL_NAME))
                .append(",'")
                .append(condition.getPattern())
                .append("')")
                .append(" or ")
                .append("jcr:like(fn:name(),'")
                .append(escape(condition.getPattern()))
                .append("')")
                .append(')');
    }

    @Override
    public void visit(Condition.Property condition) throws RepositoryException {
        RelationOp relOp = condition.getOp();
        if (relOp == RelationOp.EX) {
            xPath.append(condition.getRelPath());
        } else if (relOp == RelationOp.LIKE) {
            xPath.append("jcr:like(")
                    .append(condition.getRelPath())
                    .append(",'")
                    .append(condition.getPattern())
                    .append("')");
        } else {
            xPath.append(condition.getRelPath())
                    .append(condition.getOp().getOp())
                    .append(format(condition.getValue()));
        }
    }

    @Override
    public void visit(Condition.Contains condition) {
        xPath.append("jcr:contains(")
                .append(condition.getRelPath())
                .append(",'")
                .append(condition.getSearchExpr())
                .append("')");
    }

    @Override
    public void visit(Condition.Impersonation condition) {
        xPath.append("@rep:impersonators='")
                .append(condition.getName())
                .append('\'');
    }

    @Override
    public void visit(Condition.Not condition) throws RepositoryException {
        xPath.append("not(");
        condition.getCondition().accept(this);
        xPath.append(')');
    }

    @Override
    public void visit(Condition.And condition) throws RepositoryException {
        int count = 0;
        for (Condition c : condition) {
            xPath.append(count++ > 0 ? " and " : "");
            c.accept(this);
        }
    }

    @Override
    public void visit(Condition.Or condition) throws RepositoryException {
        int pos = xPath.length();

        int count = 0;
        for (Condition c : condition) {
            xPath.append(count++ > 0 ? " or " : "");
            c.accept(this);
        }

        // Surround or clause with parentheses if it contains more than one term
        if (count > 1) {
            xPath.insert(pos, '(');
            xPath.append(')');
        }
    }

    //------------------------------------------------------------< private >---
    /**
     * Escape {@code string} for matching in jcr escaped node names
     *
     * @param string string to escape
     * @return escaped string
     */
    @Nonnull
    public static String escape(String string) {
        StringBuilder result = new StringBuilder();

        int k = 0;
        int j;
        do {
            j = string.indexOf('%', k); // split on %
            if (j < 0) {
                // jcr escape trail
                result.append(Text.escapeIllegalJcrChars(string.substring(k)));
            } else if (j > 0 && string.charAt(j - 1) == '\\') {
                // literal occurrence of % -> jcr escape
                result.append(Text.escapeIllegalJcrChars(string.substring(k, j) + '%'));
            } else {
                // wildcard occurrence of % -> jcr escape all but %
                result.append(Text.escapeIllegalJcrChars(string.substring(k, j))).append('%');
            }

            k = j + 1;
        } while (j >= 0);

        return result.toString();
    }

    @Nonnull
    private String getNtName(Class<? extends Authorizable> selector) {
        String ntName;
        if (User.class.isAssignableFrom(selector)) {
            ntName = namePathMapper.getJcrName(UserConstants.NT_REP_USER);
        } else if (Group.class.isAssignableFrom(selector)) {
            ntName = namePathMapper.getJcrName(UserConstants.NT_REP_GROUP);
        } else {
            ntName = namePathMapper.getJcrName(UserConstants.NT_REP_AUTHORIZABLE);
        }
        if (ntName == null) {
            log.warn("Failed to retrieve JCR name for authorizable node type.");
            ntName = UserConstants.NT_REP_AUTHORIZABLE;
        }
        return ntName;
    }

    @Nonnull
    private static String format(Value value) throws RepositoryException {
        switch (value.getType()) {
            case PropertyType.STRING:
            case PropertyType.BOOLEAN:
                return '\'' + value.getString() + '\'';

            case PropertyType.LONG:
            case PropertyType.DOUBLE:
                return value.getString();

            case PropertyType.DATE:
                return "xs:dateTime('" + value.getString() + "')";

            default:
                throw new RepositoryException("Property of type " + PropertyType.nameFromValue(value.getType()) +
                        " not supported");
        }
    }

    @Nonnull
    private static RelationOp getCollation(QueryBuilder.Direction direction) throws RepositoryException {
        switch (direction) {
            case ASCENDING:
                return RelationOp.GT;
            case DESCENDING:
                return RelationOp.LT;
            default:
                throw new RepositoryException("Unknown sort order " + direction);
        }
    }

    @Nonnull
    private Iterator<Authorizable> findAuthorizables(long limit, long offset) throws ParseException {
        Iterable<? extends ResultRow> resultRows = root.getQueryEngine().executeQuery(xPath.toString(), Query.XPATH, limit, offset, null, root, namePathMapper).getRows();

        Function<ResultRow, Authorizable> transformer = new Function<ResultRow, Authorizable>() {
            public Authorizable apply(ResultRow resultRow) {
                try {
                    return userManager.getAuthorizableByPath(resultRow.getPath());
                } catch (RepositoryException e) {
                    log.warn("Cannot create authorizable from result row {}", resultRow);
                    log.debug(e.getMessage(), e);
                    return null;
                }
            }
        };

        return Iterators.filter(Iterators.transform(resultRows.iterator(), transformer), Predicates.<Object>notNull());
    }

    @Nonnull
    private Iterator<Authorizable> filter(Iterator<Authorizable> authorizables,
                                          String groupName,
                                          final boolean declaredMembersOnly) throws RepositoryException {
        Predicate<Authorizable> predicate;
        Authorizable authorizable = userManager.getAuthorizable(groupName);
        if (authorizable == null || !authorizable.isGroup()) {
            predicate = Predicates.alwaysFalse();
        } else {
            final Group group = (Group) authorizable;
            predicate = new Predicate<Authorizable>() {
                public boolean apply(Authorizable authorizable) {
                    try {
                        return (declaredMembersOnly) ? group.isDeclaredMember(authorizable) : group.isMember(authorizable);
                    } catch (RepositoryException e) {
                        log.debug("Cannot determine group membership for {}", authorizable, e.getMessage());
                        return false;
                    }
                }
            };
        }
        return Iterators.filter(authorizables, predicate);
    }
}