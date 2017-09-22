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

import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.Value;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.commons.QueryUtils;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class XPathConditionVisitorTest extends AbstractSecurityTest {

    private static final Map<String, String> LOCAL = ImmutableMap.of("rcj", "http://www.jcp.org/jcr/1.0");

    private static final String REL_PATH = "r'e/l/path";
    private static final String SERACH_EXPR = "s%e\\%arch\\E[:]xpr";

    private StringBuilder statement;
    private XPathConditionVisitor visitor;
    private Condition.Contains testCondition;

    @Override
    public void before() throws Exception {
        super.before();

        statement = new StringBuilder();
        visitor = new XPathConditionVisitor(statement, getNamePathMapper(), getUserManager(root));
        testCondition = new Condition.Contains(REL_PATH, SERACH_EXPR);
    }

    @Override
    protected NamePathMapper getNamePathMapper() {
        return new NamePathMapperImpl(new LocalNameMapper(root, LOCAL));
    }

    private static void reduceCompoundConditionToSingleTerm(@Nonnull Condition.Compound condition) {
        Iterator<Condition> it = condition.iterator();
        if (it.hasNext()) {
            it.next();
            it.remove();
        }
        assertEquals(1, Iterators.size(condition.iterator()));
    }

    @Test
    public void testVisitNode() throws Exception {
        visitor.visit(new Condition.Node(SERACH_EXPR));

        String s = statement.toString();
        assertFalse(s.contains(SERACH_EXPR));
        assertTrue(s.contains(QueryUtils.escapeForQuery(SERACH_EXPR)));
        assertTrue(s.contains(QueryUtils.escapeForQuery(QueryUtils.escapeNodeName(SERACH_EXPR))));

    }

    @Test
    public void testVisitProperty() throws Exception {
        Value v = getValueFactory(root).createValue(SERACH_EXPR);
        for (RelationOp op : RelationOp.values()) {
            if (op == RelationOp.EX || op == RelationOp.LIKE) {
                continue;
            }

            visitor.visit(new Condition.Property(REL_PATH, op, v));

            String s = statement.toString();
            String expected = QueryUtils.escapeForQuery(REL_PATH) + op.getOp() + QueryUtil.format(v);

            assertEquals(expected, s);

            // reset statement for next operation
            statement.delete(0, statement.length());
        }
    }

    @Test
    public void testVisitPropertyExists() throws Exception {
        visitor.visit((Condition.Property) new XPathQueryBuilder().exists(REL_PATH));

        assertEquals(QueryUtils.escapeForQuery(REL_PATH), statement.toString());
    }

    @Test
    public void testVisitPropertyLike() throws Exception {
        visitor.visit((Condition.Property) new XPathQueryBuilder().like(REL_PATH, SERACH_EXPR));

        String s = statement.toString();
        assertTrue(s.startsWith("jcr:like("));
        assertTrue(s.endsWith(")"));

        assertFalse(s.contains(REL_PATH));
        assertTrue(s.contains(QueryUtils.escapeForQuery(REL_PATH)));

        assertFalse(s.contains(SERACH_EXPR));
        assertTrue(s.contains(QueryUtils.escapeForQuery(SERACH_EXPR)));
    }

    @Test
    public void testVisitContains() {
        visitor.visit(testCondition);

        String s = statement.toString();
        assertTrue(s.startsWith("jcr:contains("));
        assertTrue(s.endsWith(")"));

        assertFalse(s.contains(REL_PATH));
        assertTrue(s.contains(QueryUtils.escapeForQuery(REL_PATH)));

        assertFalse(s.contains(SERACH_EXPR));
        assertTrue(s.contains(QueryUtils.escapeForQuery(SERACH_EXPR)));
    }

    @Test
    public void testVisitImpersonation() throws Exception {
        String principalName = getTestUser().getPrincipal().getName();;
        Condition.Impersonation c = new Condition.Impersonation(principalName);
        visitor.visit(c);

        String s = statement.toString();
        assertTrue(s.contains(UserConstants.REP_IMPERSONATORS));
        assertFalse(s.contains("@rcj:primaryType='" + UserConstants.NT_REP_USER + "'"));
    }

    @Test
    public void testVisitImpersonationAdmin() throws Exception {
        String adminPrincipalName = getUserManager(root).getAuthorizable(getUserConfiguration().getParameters().getConfigValue(UserConstants.PARAM_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID)).getPrincipal().getName();
        Condition.Impersonation c = new Condition.Impersonation(adminPrincipalName);
        visitor.visit(c);

        String s = statement.toString();
        assertFalse(s.contains(UserConstants.REP_IMPERSONATORS));
        assertTrue(s.contains("@rcj:primaryType='" + UserConstants.NT_REP_USER + "'"));
    }

    @Test
    public void testVisitNot() throws Exception {
        visitor.visit(new Condition.Not(testCondition));
        assertTrue(statement.toString().startsWith("not("));
        assertTrue(statement.toString().endsWith(")"));
    }

    @Test
    public void testVisitAnd() throws Exception {
        visitor.visit(new Condition.And(testCondition, testCondition));
        assertTrue(statement.toString().contains(" and "));
    }


    @Test
    public void testVisitAndSingle() throws Exception {
        Condition.And c = new Condition.And(testCondition, testCondition);
        reduceCompoundConditionToSingleTerm(c);

        visitor.visit(c);
        assertFalse(statement.toString().contains(" and "));
    }

    @Test
    public void testVisitOr() throws Exception {
        visitor.visit(new Condition.Or(testCondition, testCondition));

        String s = statement.toString();
        assertTrue(s.contains(" or "));
        assertTrue(s.startsWith("("));
        assertTrue(s.endsWith("))"));
    }

    @Test
    public void testVisitOrSingle() throws Exception {
        Condition.Or c = new Condition.Or(testCondition, testCondition);
        reduceCompoundConditionToSingleTerm(c);
        visitor.visit(c);

        String s = statement.toString();
        assertFalse(s.contains(" or "));
        assertFalse(s.startsWith("("));
        assertFalse(s.endsWith("))"));
    }
}