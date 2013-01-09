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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * XPATH based evaluation of {@link org.apache.jackrabbit.api.security.user.Query}s.
 */
public class XPathQueryEvaluator implements ConditionVisitor {

    static final Logger log = LoggerFactory.getLogger(XPathQueryEvaluator.class);

    private final XPathQueryBuilder builder;
    private final UserManager userManager;
    private final Root root;
    private final NamePathMapper namePathMapper;
    private final ConfigurationParameters config;

    private final StringBuilder statement = new StringBuilder();

    public XPathQueryEvaluator(XPathQueryBuilder builder, UserManager userManager,
                               Root root, NamePathMapper namePathMapper,
                               ConfigurationParameters config) {
        this.builder = builder;
        this.userManager = userManager;
        this.root = root;
        this.namePathMapper = namePathMapper;
        this.config = config;
    }

    public Iterator<Authorizable> eval() throws RepositoryException {
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
                Condition boundCondition = builder.property(sortCol, getCollation(sortDir), bound);
                if (condition == null) {
                    condition = boundCondition;
                } else {
                    condition = builder.and(condition, boundCondition);
                }
            }
        }

        String searchRoot = QueryUtil.getSearchRoot(builder.getSelectorType(), config);
        String ntName = QueryUtil.getNodeTypeName(builder.getSelectorType());
        statement.append(searchRoot).append("//element(*,").append(ntName).append(')');

        if (condition != null) {
            statement.append('[');
            condition.accept(this);
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

        try {
            if (builder.getGroupName() == null) {
                long offset = builder.getOffset();
                if (bound != null && offset > 0) {
                    log.warn("Found bound {} and offset {} in limit. Discarding offset.", bound, offset);
                    offset = 0;
                }
                return findAuthorizables(builder.getMaxCount(), offset);
            } else {
                // filtering by group name included in query -> enforce offset
                // and limit on the result set.
                Iterator<Authorizable> result = findAuthorizables(Long.MAX_VALUE, 0);
                Iterator<Authorizable> filtered = filter(result, builder.getGroupName(), builder.isDeclaredMembersOnly());
                return ResultIterator.create(builder.getOffset(), builder.getMaxCount(), filtered);
            }
        } catch (ParseException e) {
            throw new RepositoryException(e);
        }
    }

    //---------------------------------------------------< ConditionVisitor >---
    @Override
    public void visit(Condition.Node condition) throws RepositoryException {
        statement.append('(')
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
            statement.append(condition.getRelPath());
        } else if (relOp == RelationOp.LIKE) {
            statement.append("jcr:like(")
                    .append(condition.getRelPath())
                    .append(",'")
                    .append(condition.getPattern())
                    .append("')");
        } else {
            statement.append(condition.getRelPath())
                    .append(condition.getOp().getOp())
                    .append(format(condition.getValue()));
        }
    }

    @Override
    public void visit(Condition.Contains condition) {
        statement.append("jcr:contains(")
                .append(condition.getRelPath())
                .append(",'")
                .append(condition.getSearchExpr())
                .append("')");
    }

    @Override
    public void visit(Condition.Impersonation condition) {
        statement.append("@rep:impersonators='")
                .append(condition.getName())
                .append('\'');
    }

    @Override
    public void visit(Condition.Not condition) throws RepositoryException {
        statement.append("not(");
        condition.getCondition().accept(this);
        statement.append(')');
    }

    @Override
    public void visit(Condition.And condition) throws RepositoryException {
        int count = 0;
        for (Condition c : condition) {
            statement.append(count++ > 0 ? " and " : "");
            c.accept(this);
        }
    }

    @Override
    public void visit(Condition.Or condition) throws RepositoryException {
        int pos = statement.length();

        int count = 0;
        for (Condition c : condition) {
            statement.append(count++ > 0 ? " or " : "");
            c.accept(this);
        }

        // Surround or clause with parentheses if it contains more than one term
        if (count > 1) {
            statement.insert(pos, '(');
            statement.append(')');
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
            j = string.indexOf('%', k);
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
                throw new RepositoryException("Property of type " + PropertyType.nameFromValue(value.getType()) + " not supported");
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
        Iterable<? extends ResultRow> resultRows = root.getQueryEngine().executeQuery(statement.toString(), Query.XPATH, limit, offset, null, namePathMapper).getRows();
        Iterator<Authorizable> authorizables = Iterators.transform(resultRows.iterator(), new ResultRowToAuthorizable(userManager));
        return Iterators.filter(authorizables, Predicates.<Object>notNull());
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