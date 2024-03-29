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

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.security.user.Utils.canImpersonateAllUsers;

/**
 * XPATH based condition visitor.
 */
class XPathConditionVisitor implements ConditionVisitor {

    private final StringBuilder statement;
    private final NamePathMapper namePathMapper;
    private final UserManager userMgr;

    XPathConditionVisitor(@NotNull StringBuilder statement, @NotNull NamePathMapper namePathMapper,
                          @NotNull UserManager userMgr) {
        this.statement = statement;
        this.namePathMapper = namePathMapper;
        this.userMgr = userMgr;
    }

    //---------------------------------------------------< ConditionVisitor >---
    @Override
    public void visit(@NotNull Condition.Node condition) {
        statement.append('(')
                .append("jcr:like(@")
                .append(QueryUtil.escapeForQuery(UserConstants.REP_AUTHORIZABLE_ID, namePathMapper))
                .append(",'")
                .append(QueryUtil.escapeForQuery(condition.getPattern()))
                .append("')")
                .append(" or ")
                .append("jcr:like(@")
                .append(QueryUtil.escapeForQuery(UserConstants.REP_PRINCIPAL_NAME, namePathMapper))
                .append(",'")
                .append(QueryUtil.escapeForQuery(condition.getPattern()))
                .append("')")
                .append(" or ")
                .append("jcr:like(fn:name(),'")
                .append(QueryUtil.escapeForQuery(QueryUtil.escapeNodeName(condition.getPattern())))
                .append("')")
                .append(')');
    }

    @Override
    public void visit(@NotNull Condition.PropertyValue condition) throws RepositoryException {
        statement.append(QueryUtil.escapeForQuery(condition.getRelPath()))
                .append(condition.getOp().getOp())
                .append(QueryUtil.format(condition.getValue()));
    }

    @Override
    public void visit(Condition.@NotNull PropertyLike condition) {
        statement.append("jcr:like(")
                .append(QueryUtil.escapeForQuery(condition.getRelPath()))
                .append(",'")
                .append(QueryUtil.escapeForQuery(condition.getPattern()))
                .append("')");
    }

    @Override
    public void visit(Condition.@NotNull PropertyExists condition) {
        statement.append(QueryUtil.escapeForQuery(condition.getRelPath()));
    }

    @Override
    public void visit(@NotNull Condition.Contains condition) {
        statement.append("jcr:contains(")
                .append(QueryUtil.escapeForQuery(condition.getRelPath()))
                .append(",'")
                .append(QueryUtil.escapeForQuery(condition.getSearchExpr()))
                .append("')");
    }

    @Override
    public void visit(@NotNull Condition.Impersonation condition) {
        String principalName = condition.getName();
        if (canImpersonateAllUsers(new PrincipalImpl(principalName), userMgr)) {
            statement.append('@')
                    .append(QueryUtil.escapeForQuery(JcrConstants.JCR_PRIMARYTYPE, namePathMapper))
                    .append("='")
                    .append(QueryUtil.escapeForQuery(UserConstants.NT_REP_USER, namePathMapper))
                    .append('\'');
        } else {
            statement.append('@')
                    .append(QueryUtil.escapeForQuery(UserConstants.REP_IMPERSONATORS, namePathMapper))
                    .append("='")
                    .append(condition.getName())
                    .append('\'');
        }
    }

    @Override
    public void visit(@NotNull Condition.Not condition) throws RepositoryException {
        statement.append("not(");
        condition.getCondition().accept(this);
        statement.append(')');
    }

    @Override
    public void visit(@NotNull Condition.And condition) throws RepositoryException {
        int count = 0;
        for (Condition c : condition) {
            statement.append(count++ > 0 ? " and " : "");
            c.accept(this);
        }
    }

    @Override
    public void visit(@NotNull Condition.Or condition) throws RepositoryException {
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
}