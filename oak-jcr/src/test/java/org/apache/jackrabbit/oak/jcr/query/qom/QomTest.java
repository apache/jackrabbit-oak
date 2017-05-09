/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.jcr.query.qom;

import static org.junit.Assert.assertEquals;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeType;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.qom.And;
import javax.jcr.query.qom.BindVariableValue;
import javax.jcr.query.qom.ChildNode;
import javax.jcr.query.qom.ChildNodeJoinCondition;
import javax.jcr.query.qom.Column;
import javax.jcr.query.qom.Comparison;
import javax.jcr.query.qom.Constraint;
import javax.jcr.query.qom.DescendantNode;
import javax.jcr.query.qom.DescendantNodeJoinCondition;
import javax.jcr.query.qom.EquiJoinCondition;
import javax.jcr.query.qom.FullTextSearch;
import javax.jcr.query.qom.FullTextSearchScore;
import javax.jcr.query.qom.Join;
import javax.jcr.query.qom.Length;
import javax.jcr.query.qom.Literal;
import javax.jcr.query.qom.LowerCase;
import javax.jcr.query.qom.NodeLocalName;
import javax.jcr.query.qom.NodeName;
import javax.jcr.query.qom.Not;
import javax.jcr.query.qom.Or;
import javax.jcr.query.qom.Ordering;
import javax.jcr.query.qom.PropertyExistence;
import javax.jcr.query.qom.PropertyValue;
import javax.jcr.query.qom.QueryObjectModel;
import javax.jcr.query.qom.QueryObjectModelConstants;
import javax.jcr.query.qom.QueryObjectModelFactory;
import javax.jcr.query.qom.SameNode;
import javax.jcr.query.qom.SameNodeJoinCondition;
import javax.jcr.query.qom.Selector;
import javax.jcr.query.qom.Source;
import javax.jcr.query.qom.UpperCase;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the QueryObjectModelFactory and other QOM classes.
 */
public class QomTest extends AbstractRepositoryTest {

    private ValueFactory vf;
    private QueryObjectModelFactory f;

    public QomTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void before() throws RepositoryException {
        Session session = getAdminSession();
        vf = session.getValueFactory();
        QueryManager qm = session.getWorkspace().getQueryManager();
        f = qm.getQOMFactory();
    }
    
    @Test
    public void jcrNameConversion() throws RepositoryException {
        assertEquals("[nt:base]", 
                f.column(null, NodeType.NT_BASE, null).toString());
        assertEquals("[s1].[nt:base] = [s2].[nt:base]", 
                f.equiJoinCondition("s1", NodeType.NT_BASE, "s2", NodeType.NT_BASE).toString());
        assertEquals("CONTAINS([nt:base], null)", 
                f.fullTextSearch(null, NodeType.NT_BASE, null).toString());
        assertEquals("CAST('nt:base' AS NAME)", 
                f.literal(vf.createValue(NodeType.NT_BASE, PropertyType.NAME)).toString());
        assertEquals("[nt:base] IS NOT NULL", 
                f.propertyExistence(null, NodeType.NT_BASE).toString());
        assertEquals("[nt:base]", 
                f.propertyValue(null, NodeType.NT_BASE).toString());
        assertEquals("[nt:base]", 
                f.selector(NodeType.NT_BASE, null).toString());
        
        Source source1 = f.selector(NodeType.NT_BASE, "selector");
        Column[] columns = new Column[] { f.column("selector", null, null) };
        Constraint constraint2 = f.childNode("selector", "/");
        QueryObjectModel qom = f.createQuery(source1, constraint2, null,
                columns);
        assertEquals("select [selector].* from " + 
                "[nt:base] AS [selector] " + 
                "where ISCHILDNODE([selector], [/])", qom.toString());
    }

    @Test
    public void and() throws RepositoryException {
        Constraint c0 = f.propertyExistence("x", "c0");
        Constraint c1 = f.propertyExistence("x", "c1");
        And and = f.and(c0, c1);
        assertEquals(and.getConstraint1(), c0);
        assertEquals(and.getConstraint2(), c1);
        assertEquals("([x].[c0] IS NOT NULL) AND ([x].[c1] IS NOT NULL)", and.toString());
    }

    @Test
    public void ascending() throws RepositoryException {
        PropertyValue p = f.propertyValue("selectorName", "propertyName");
        Ordering o = f.ascending(p);
        assertEquals(p, o.getOperand());
        assertEquals(QueryObjectModelConstants.JCR_ORDER_ASCENDING, o.getOrder());
        assertEquals("[selectorName].[propertyName]", p.toString());
    }

    @Test
    public void bindVariable() throws RepositoryException {
        BindVariableValue b = f.bindVariable("bindVariableName");
        assertEquals("bindVariableName", b.getBindVariableName());
        assertEquals("$bindVariableName", b.toString());
    }

    @Test
    public void childNode() throws RepositoryException {
        ChildNode cn = f.childNode("selectorName", "parentPath");
        assertEquals("selectorName", cn.getSelectorName());
        assertEquals("parentPath", cn.getParentPath());
        assertEquals("ISCHILDNODE([selectorName], [parentPath])", cn.toString());
        
        assertEquals("ISCHILDNODE([p])", f.childNode(null, "p").toString());
    }

    @Test
    public void childNodeJoinCondition() throws RepositoryException {
        ChildNodeJoinCondition c = f.childNodeJoinCondition("childSelectorName",
                "parentSelectorName");
        assertEquals("childSelectorName", c.getChildSelectorName());
        assertEquals("parentSelectorName", c.getParentSelectorName());
        assertEquals("ISCHILDNODE([childSelectorName], [parentSelectorName])",
                c.toString());
    }

    @Test
    public void column() throws RepositoryException {
        Column c = f.column("selectorName", "propertyName", "columnName");
        assertEquals("selectorName", c.getSelectorName());
        assertEquals("propertyName", c.getPropertyName());
        assertEquals("columnName", c.getColumnName());
        assertEquals("[selectorName].[propertyName] AS [columnName]", c.toString());
        
        assertEquals("[p]", f.column(null, "p", null).toString());
        assertEquals("[p] AS [c]", f.column(null, "p", "c").toString());
        assertEquals("[s].[p]", f.column("s", "p", null).toString());
        assertEquals("[s].[p] AS [c]", f.column("s", "p", "c").toString());
        assertEquals("[s].* AS [c]", f.column("s", null, "c").toString());
        assertEquals("* AS [c]", f.column(null, null, "c").toString());
        assertEquals("*", f.column(null, null, null).toString());
        assertEquals("[s].*", f.column("s", null, null).toString());
    }

    @Test
    public void comparison() throws RepositoryException {
        PropertyValue p = f.propertyValue("selectorName", "propertyName");
        Literal l = f.literal(vf.createValue(1));
        Comparison c = f.comparison(p, QueryObjectModelConstants.JCR_OPERATOR_EQUAL_TO, l);
        assertEquals(p, c.getOperand1());
        assertEquals(QueryObjectModelConstants.JCR_OPERATOR_EQUAL_TO, c.getOperator());
        assertEquals(l, c.getOperand2());
        assertEquals("[selectorName].[propertyName] = 1", c.toString());
    }

    @Test
    public void descendantNode() throws RepositoryException {
        DescendantNode d = f.descendantNode("selectorName", "path");
        assertEquals("selectorName", d.getSelectorName());
        assertEquals("path", d.getAncestorPath());
        assertEquals("ISDESCENDANTNODE([selectorName], [path])", d.toString());
        
        assertEquals("ISDESCENDANTNODE([p])", 
                f.descendantNode(null, "p").toString());
    }

    @Test
    public void descendantNodeJoinCondition() throws RepositoryException {
        DescendantNodeJoinCondition d = f.descendantNodeJoinCondition("descendantSelectorName",
                "ancestorSelectorName");
        assertEquals("descendantSelectorName", d.getDescendantSelectorName());
        assertEquals("ancestorSelectorName", d.getAncestorSelectorName());
        assertEquals("ISDESCENDANTNODE([descendantSelectorName], [ancestorSelectorName])",
                d.toString());
    }

    @Test
    public void descending() throws RepositoryException {
        PropertyValue p = f.propertyValue("selectorName", "propertyName");
        Ordering o = f.descending(p);
        assertEquals(p, o.getOperand());
        assertEquals(QueryObjectModelConstants.JCR_ORDER_DESCENDING, o.getOrder());
        assertEquals("[selectorName].[propertyName] DESC", o.toString());
    }

    @Test
    public void equiJoinCondition() throws RepositoryException {
        EquiJoinCondition e = f.equiJoinCondition("selector1Name", "property1Name",
                "selector2Name", "property2Name");
        assertEquals("selector1Name", e.getSelector1Name());
        assertEquals("property1Name", e.getProperty1Name());
        assertEquals("selector2Name", e.getSelector2Name());
        assertEquals("property2Name", e.getProperty2Name());
        assertEquals("[selector1Name].[property1Name] = [selector2Name].[property2Name]",
                e.toString());
    }

    @Test
    public void fullTextSearch() throws RepositoryException {
        Literal l = f.literal(vf.createValue(1));
        FullTextSearch x = f.fullTextSearch("selectorName", "propertyName", l);
        assertEquals("selectorName", x.getSelectorName());
        assertEquals("propertyName", x.getPropertyName());
        assertEquals(l, x.getFullTextSearchExpression());
        assertEquals("CONTAINS([selectorName].[propertyName], 1)", x.toString());
        
        assertEquals("CONTAINS([p], null)", f.fullTextSearch(null,  "p",  null).toString());
        assertEquals("CONTAINS([s].[p], null)", f.fullTextSearch("s",  "p",  null).toString());
        assertEquals("CONTAINS([s].*, null)", f.fullTextSearch("s",  null,  null).toString());
        assertEquals("CONTAINS(*, null)", f.fullTextSearch(null,  null,  null).toString());
    }

    @Test
    public void fullTextSearchScore() throws RepositoryException {
        FullTextSearchScore x = f.fullTextSearchScore("selectorName");
        assertEquals("selectorName", x.getSelectorName());
        assertEquals("SCORE([selectorName])", x.toString());
        
        assertEquals("SCORE()", f.fullTextSearchScore(null).toString());

    }

    @Test
    public void join() throws RepositoryException {
        Source left = f.selector("nodeTypeName", "selectorName");
        Source right = f.selector("nodeTypeName2", "selectorName2");
        ChildNodeJoinCondition jc = f.childNodeJoinCondition("childSelectorName", "parentSelectorName");
        Join j = f.join(left, right, QueryObjectModelConstants.JCR_JOIN_TYPE_INNER, jc);
        assertEquals(left, j.getLeft());
        assertEquals(right, j.getRight());
        assertEquals(QueryObjectModelConstants.JCR_JOIN_TYPE_INNER, j.getJoinType());
        assertEquals(jc, j.getJoinCondition());
        assertEquals("ISCHILDNODE([childSelectorName], [parentSelectorName])", jc.toString());
    }

    @Test
    public void length() throws RepositoryException {
        PropertyValue p = f.propertyValue("selectorName", "propertyName");
        Length l = f.length(p);
        assertEquals(p, l.getPropertyValue());
        assertEquals("LENGTH([selectorName].[propertyName])", l.toString());
    }

    @Test
    public void literal() throws RepositoryException {
        Value v = vf.createValue(1);
        Literal l = f.literal(v);
        assertEquals(v, l.getLiteralValue());
        assertEquals("1", l.toString());
        assertEquals("'Joe''s'", f.literal(vf.createValue("Joe's")).toString());
        assertEquals("' - \" - '", f.literal(vf.createValue(" - \" - ")).toString());
    }

    @Test
    public void lowerCase() throws RepositoryException {
        PropertyValue p = f.propertyValue("selectorName", "propertyName");
        Length length = f.length(p);
        LowerCase l = f.lowerCase(length);
        assertEquals(length, l.getOperand());
        assertEquals("LOWER(LENGTH([selectorName].[propertyName]))", l.toString());
    }

    @Test
    public void nodeLocalName() throws RepositoryException {
        NodeLocalName n = f.nodeLocalName("selectorName");
        assertEquals("selectorName", n.getSelectorName());
        assertEquals("LOCALNAME([selectorName])", n.toString());
        assertEquals("LOCALNAME()", f.nodeLocalName(null).toString());
    }

    @Test
    public void nodeName() throws RepositoryException {
        NodeName n = f.nodeName("selectorName");
        assertEquals("selectorName", n.getSelectorName());
        assertEquals("NAME([selectorName])", n.toString());
        assertEquals("NAME()", f.nodeName(null).toString());
    }

    @Test
    public void not() throws RepositoryException {
        Constraint c = f.propertyExistence("x", "c0");
        Not n = f.not(c);
        assertEquals(c, n.getConstraint());
        assertEquals("[x].[c0] IS NOT NULL", c.toString());
        
        assertEquals("* IS NOT NULL", f.propertyExistence(null, null).toString());
        assertEquals("[s].* IS NOT NULL", f.propertyExistence("s", null).toString());
        assertEquals("[p] IS NOT NULL", f.propertyExistence(null, "p").toString());
        assertEquals("[s].[p] IS NOT NULL", f.propertyExistence("s", "p").toString());
    }

    @Test
    public void or() throws RepositoryException {
        Constraint c0 = f.propertyExistence("x", "c0");
        Constraint c1 = f.propertyExistence("x", "c1");
        Or or = f.or(c0, c1);
        assertEquals(or.getConstraint1(), c0);
        assertEquals(or.getConstraint2(), c1);
        assertEquals("([x].[c0] IS NOT NULL) OR ([x].[c1] IS NOT NULL)", or.toString());
    }

    @Test
    public void propertyExistence() throws RepositoryException {
        PropertyExistence pe = f.propertyExistence("selectorName", "propertyName");
        assertEquals("selectorName", pe.getSelectorName());
        assertEquals("propertyName", pe.getPropertyName());
        assertEquals("[selectorName].[propertyName] IS NOT NULL", pe.toString());
        
        assertEquals("* IS NOT NULL", 
                f.propertyExistence(null, null).toString());
        assertEquals("[s].* IS NOT NULL", 
                f.propertyExistence("s", null).toString());
        assertEquals("[p] IS NOT NULL", 
                f.propertyExistence(null, "p").toString());
        assertEquals("[s].[p] IS NOT NULL", 
                f.propertyExistence("s", "p").toString());
    }

    @Test
    public void propertyValue() throws RepositoryException {
        PropertyValue pv = f.propertyValue("selectorName", "propertyName");
        assertEquals("selectorName", pv.getSelectorName());
        assertEquals("propertyName", pv.getPropertyName());
        assertEquals("[selectorName].[propertyName]", pv.toString());
        
        assertEquals("*", f.propertyValue(null, null).toString());
        assertEquals("[s].*", f.propertyValue("s", null).toString());
        assertEquals("[p]", f.propertyValue(null, "p").toString());
        assertEquals("[s].[p]", f.propertyValue("s", "p").toString());
    }

    @Test
    public void sameNode() throws RepositoryException {
        SameNode s = f.sameNode("selectorName", "path");
        assertEquals("selectorName", s.getSelectorName());
        assertEquals("path", s.getPath());
        assertEquals("ISSAMENODE([selectorName], [path])", s.toString());
        
        assertEquals("ISSAMENODE([path])", f.sameNode(null, "path").toString());
        assertEquals("ISSAMENODE([s], [path])", f.sameNode("s", "path").toString());

    }

    @Test
    public void sameNodeJoinCondition() throws RepositoryException {
        SameNodeJoinCondition s = f.sameNodeJoinCondition("selector1Name", "selector2Name", "selector2Path");
        assertEquals("selector1Name", s.getSelector1Name());
        assertEquals("selector2Name", s.getSelector2Name());
        assertEquals("selector2Path", s.getSelector2Path());
        assertEquals("ISSAMENODE([selector1Name], [selector2Name], [selector2Path])",
                s.toString());
    }

    @Test
    public void selector() throws RepositoryException {
        Selector s = f.selector("nodeTypeName", "selectorName");
        assertEquals("nodeTypeName", s.getNodeTypeName());
        assertEquals("selectorName", s.getSelectorName());
        assertEquals("[nodeTypeName] AS [selectorName]", s.toString());
        assertEquals("[n]", f.selector("n",  null).toString());
    }

    @Test
    public void upperCase() throws RepositoryException {
        PropertyValue p = f.propertyValue("selectorName", "propertyName");
        Length length = f.length(p);
        UpperCase u = f.upperCase(length);
        assertEquals(length, u.getOperand());
        assertEquals("UPPER(LENGTH([selectorName].[propertyName]))", u.toString());
    }

    @Test
    public void createQuery() throws RepositoryException {
        Selector s = f.selector("nt:file", "x");
        BindVariableValue b = f.bindVariable("var");
        Constraint c = f.propertyExistence("x", "c");
        PropertyValue p = f.propertyValue("x", "propertyName");
        c = f.and(f.comparison(p, QueryObjectModelConstants.JCR_OPERATOR_EQUAL_TO, b), c);
        Ordering o = f.ascending(p);
        Column col = f.column("x", "propertyName", "columnName");
        Ordering[] ords = new Ordering[]{o};
        Column[] cols = new Column[]{col};
        QueryObjectModel q = f.createQuery(s, c, ords, cols);
        assertEquals(Query.JCR_JQOM, q.getLanguage());
        String[] bv = q.getBindVariableNames();
        assertEquals(1, bv.length);
        assertEquals("var", bv[0]);
        assertEquals(s, q.getSource());
        assertEquals(c, q.getConstraint());
        assertEquals(o, q.getOrderings()[0]);
        assertEquals(col, q.getColumns()[0]);
    }
    
    @Test
    public void escapedName() throws RepositoryException {
        assertEquals("[[n]]]", f.selector("[n]",  null).toString());
        assertEquals("[[s]]].[[p]]]", f.propertyValue("[s]", "[p]").toString());
        assertEquals("ISSAMENODE([[s1]]], [[s2]]], [[p]]])", 
                f.sameNodeJoinCondition("[s1]", "[s2]", "[p]").toString());        
        assertEquals("ISSAMENODE([[s]]], [[p]]])", 
                f.sameNode("[s]", "[p]").toString());        
    }
}
