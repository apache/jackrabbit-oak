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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.Value;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class XPathQueryBuilderTest extends AbstractSecurityTest {

    private XPathQueryBuilder builder = new XPathQueryBuilder();

    private final Value v = Mockito.mock(Value.class);
    private final String relPath = "re/l/path";

    private void assertPropertyCondition(@Nonnull Condition condition, @Nonnull RelationOp expectedOp) {
        assertTrue(condition instanceof Condition.Property);
        Condition.Property cp = (Condition.Property) condition;

        assertSame(expectedOp, cp.getOp());

        assertEquals(relPath, cp.getRelPath());
        assertEquals(v, cp.getValue());
        assertNull(cp.getPattern());

        assertNull(builder.getCondition());
    }

    @Test
    public void testGetSortIgnoreCase() {
        assertFalse(builder.getSortIgnoreCase());
    }

    @Test
    public void testGetSortDirection() {
        assertSame(QueryBuilder.Direction.ASCENDING, builder.getSortDirection());
    }

    @Test
    public void testGetSortProperty() {
        assertNull(builder.getSortProperty());
    }

    @Test
    public void testSetSortOrder() {
        builder.setSortOrder("propName", QueryBuilder.Direction.DESCENDING);

        assertEquals("propName", builder.getSortProperty());
        assertSame(QueryBuilder.Direction.DESCENDING, builder.getSortDirection());
        assertFalse(builder.getSortIgnoreCase());
    }

    @Test
    public void testSetSortOrder2() {
        builder.setSortOrder("propName", QueryBuilder.Direction.DESCENDING, true);

        assertEquals("propName", builder.getSortProperty());
        assertSame(QueryBuilder.Direction.DESCENDING, builder.getSortDirection());
        assertTrue(builder.getSortIgnoreCase());

    }

    @Test
    public void testGetSelectorType() {
        assertSame(AuthorizableType.AUTHORIZABLE, builder.getSelectorType());
    }

    @Test
    public void testSetSelector() throws Exception {
        Map<Class<? extends Authorizable>, AuthorizableType> m = new HashMap();
        m.put(User.class, AuthorizableType.USER);
        m.put(getTestUser().getClass(), AuthorizableType.USER);
        m.put(Authorizable.class, AuthorizableType.AUTHORIZABLE);
        m.put(Group.class, AuthorizableType.GROUP);
        m.put(getUserManager(root).createGroup("testGroup").getClass(), AuthorizableType.GROUP);

        for (Class<? extends Authorizable> cl : m.keySet()) {
            builder.setSelector(cl);
            assertSame(m.get(cl), builder.getSelectorType());
        }
    }

    @Test
    public void testGetGroupId() {
        assertNull(builder.getGroupID());
    }

    @Test
    public void testIsDeclaredMembersOnly() {
        assertFalse(builder.isDeclaredMembersOnly());
    }

    @Test
    public void testSetScope() {
        builder.setScope("gr", true);

        assertEquals("gr", builder.getGroupID());
        assertTrue(builder.isDeclaredMembersOnly());
    }

    @Test
    public void testGetOffset() {
        assertEquals(Long.MAX_VALUE, builder.getMaxCount());
    }

    @Test
    public void testSetOffset() {
        builder.setLimit(25, -1);
        assertEquals(25, builder.getOffset());
    }

    @Test
    public void testSetOffsetResetsBoundLimit() {
        builder.setLimit(Mockito.mock(Value.class), -1);
        builder.setLimit(25, -1);
        assertNull(builder.getBound());
    }

    @Test
    public void testGetBound() {
        assertNull(builder.getBound());
    }

    @Test
    public void testSetBound() {
        builder.setLimit(v, -1);
        assertEquals(v, builder.getBound());
    }

    @Test
    public void testSetBoundResetsOffset() {
        builder.setLimit(25, -1);
        builder.setLimit(v, -1);
        assertEquals(0, builder.getOffset());
    }

    @Test
    public void testGetMaxCount() {
        assertEquals(Long.MAX_VALUE, builder.getMaxCount());
    }

    @Test
    public void testSetMaxCountMinusOne() {
        builder.setLimit(0, -1);
        assertEquals(Long.MAX_VALUE, builder.getMaxCount());
    }

    @Test
    public void testSetMaxCount() {
        builder.setLimit(0, 25);
        assertEquals(25, builder.getMaxCount());
    }

    @Test
    public void testNameMatches() {
        Condition c = builder.nameMatches("pattern");

        assertTrue(c instanceof Condition.Node);
        assertEquals("pattern", ((Condition.Node) c).getPattern());
    }

    @Test
    public void testNeq() {
        Condition c = builder.neq(relPath, v);
        assertPropertyCondition(c, RelationOp.NE);

    }

    @Test
    public void testEq() {
        Condition c = builder.eq(relPath, v);
        assertPropertyCondition(c, RelationOp.EQ);
    }

    @Test
    public void testLt() {
        Condition c = builder.lt(relPath, v);
        assertPropertyCondition(c, RelationOp.LT);
    }

    @Test
    public void testLe() {
        Condition c = builder.le(relPath, v);
        assertPropertyCondition(c, RelationOp.LE);
    }

    @Test
    public void testGt() {
        Condition c = builder.gt(relPath, v);
        assertPropertyCondition(c, RelationOp.GT);
    }

    @Test
    public void testGe() {
        Condition c = builder.ge(relPath, v);
        assertPropertyCondition(c, RelationOp.GE);
    }

    @Test
    public void testExists() {
        Condition c = builder.exists(relPath);

        assertTrue(c instanceof Condition.Property);
        Condition.Property cp = (Condition.Property) c;

        assertSame(RelationOp.EX, cp.getOp());

        assertEquals(relPath, cp.getRelPath());
        assertNull(cp.getValue());
        assertNull(cp.getPattern());
    }

    @Test
    public void testLike() {
        Condition c = builder.like(relPath, "pattern");

        assertTrue(c instanceof Condition.Property);
        Condition.Property cp = (Condition.Property) c;

        assertSame(RelationOp.LIKE, cp.getOp());

        assertEquals(relPath, cp.getRelPath());
        assertNull(cp.getValue());
        assertEquals("pattern", cp.getPattern());
    }

    @Test
    public void testContains() {
        Condition c = builder.contains(relPath, "searchEx");

        assertTrue(c instanceof Condition.Contains);
        Condition.Contains ct = (Condition.Contains) c;

        assertEquals(relPath, ct.getRelPath());
        assertEquals("searchEx", ct.getSearchExpr());
    }

    @Test
    public void testImpersonates() {
        Condition c = builder.impersonates("name");

        assertTrue(c instanceof Condition.Impersonation);
        Condition.Impersonation ci = (Condition.Impersonation) c;

        assertEquals("name", ci.getName());
    }

    @Test
    public void testNot() {
        Condition condition = builder.exists(relPath);
        Condition c = builder.not(condition);

        assertTrue(c instanceof Condition.Not);
        Condition.Not cn = (Condition.Not) c;

        assertEquals(condition, cn.getCondition());
    }

    @Test
    public void testAnd() {
        Condition condition = builder.exists(relPath);
        Condition condition2 = builder.lt(relPath, v);

        Condition c = builder.and(condition, condition2);

        assertTrue(c instanceof Condition.And);
    }

    @Test
    public void testOr() {
        Condition condition = builder.exists(relPath);
        Condition condition2 = builder.lt(relPath, v);

        Condition c = builder.or(condition, condition2);

        assertTrue(c instanceof Condition.Or);
    }

    @Test
    public void testGetCondition() {
        assertNull(builder.getCondition());

        Condition condition = builder.exists(relPath);
        builder.setCondition(condition);
        assertEquals(condition, builder.getCondition());
    }

    @Test
    public void testPropertyCondition() {
        Condition c = builder.property(relPath, RelationOp.GT, v);
        assertPropertyCondition(c, RelationOp.GT);
    }
}