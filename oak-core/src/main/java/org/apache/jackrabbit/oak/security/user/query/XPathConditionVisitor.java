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

import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * XPATH based condition visitor.
 */
class XPathConditionVisitor implements ConditionVisitor {

    static final Logger log = LoggerFactory.getLogger(XPathConditionVisitor.class);

    private final StringBuilder statement;
    private final NamePathMapper namePathMapper;

    XPathConditionVisitor(StringBuilder statement, NamePathMapper namePathMapper) {
        this.statement = statement;
        this.namePathMapper = namePathMapper;
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
                .append(QueryUtil.escapeNodeName(condition.getPattern()))
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
                    .append(QueryUtil.format(condition.getValue()));
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
}