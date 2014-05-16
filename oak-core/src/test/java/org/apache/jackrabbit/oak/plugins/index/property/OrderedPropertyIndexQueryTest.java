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
package org.apache.jackrabbit.oak.plugins.index.property;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.junit.Assert.assertNotNull;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Ignore;
import org.junit.Test;

public class OrderedPropertyIndexQueryTest extends BasicOrderedPropertyIndexQueryTest {
    private static final EditorHook HOOK = new EditorHook(new IndexUpdateProvider(
        new OrderedPropertyIndexEditorProvider()));

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        IndexUtils.createIndexDefinition(new NodeUtil(index.getChild(IndexConstants.INDEX_DEFINITIONS_NAME)),
            TEST_INDEX_NAME, false, new String[] { ORDERED_PROPERTY }, null, OrderedIndex.TYPE);
        root.commit();
    }

    /**
     * Query the index for retrieving all the entries
     *
     * @throws CommitFailedException
     * @throws ParseException
     * @throws RepositoryException
     */
    @Test
    public void queryAllEntries() throws CommitFailedException, ParseException, RepositoryException {
        setTravesalEnabled(false);

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        List<ValuePathTuple> nodes = addChildNodes(generateOrderedValues(NUMBER_OF_NODES), test,
            OrderDirection.ASC, Type.STRING);
        root.commit();

        // querying
        Iterator<? extends ResultRow> results;
        results = executeQuery(String.format("SELECT * from [%s] WHERE foo IS NOT NULL", NT_UNSTRUCTURED), SQL2, null)
            .getRows().iterator();
        assertRightOrder(nodes, results);

        setTravesalEnabled(true);
    }

    /**
     * test the index for returning the items related to a single key
     *
     * @throws CommitFailedException
     * @throws ParseException
     */
    @Test
    public void queryOneKey() throws CommitFailedException, ParseException {
        setTravesalEnabled(false);

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        List<ValuePathTuple> nodes = addChildNodes(generateOrderedValues(NUMBER_OF_NODES), test,
            OrderDirection.ASC, Type.STRING);
        root.commit();

        // getting the middle of the random list of nodes.
        ValuePathTuple searchfor = nodes.get(NUMBER_OF_NODES / 2);

        Map<String, PropertyValue> filter = ImmutableMap.of(ORDERED_PROPERTY,
            PropertyValues.newString(searchfor.getValue()));
        String query = "SELECT * FROM [%s] WHERE %s=$%s";
        Iterator<? extends ResultRow> results = executeQuery(
            String.format(query, NT_UNSTRUCTURED, ORDERED_PROPERTY, ORDERED_PROPERTY), SQL2, filter)
            .getRows().iterator();
        assertTrue("one element is expected", results.hasNext());
        assertEquals("wrong path returned", searchfor.getPath(), results.next().getPath());
        assertFalse("there should be not any more items", results.hasNext());

        setTravesalEnabled(true);
    }

    /**
     * test the range query in case of '>' condition
     *
     * @throws CommitFailedException
     * @throws ParseException
     */
    @Test
    public void queryGreaterThan() throws CommitFailedException, ParseException {
        setTravesalEnabled(false);

        final OrderDirection direction = OrderDirection.ASC;
        final String query = "SELECT * FROM [nt:base] AS n WHERE n.%s > $%s";

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initialising the data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        Calendar start = midnightFirstJan2013();
        List<ValuePathTuple> nodes = addChildNodes(
                generateOrderedDates(NUMBER_OF_NODES, direction, start), test, direction, Type.DATE);
        root.commit();

        Calendar searchForCalendar = (Calendar) start.clone();
        searchForCalendar.add(Calendar.HOUR_OF_DAY, 36);
        String searchFor = ISO_8601_2000.format(searchForCalendar.getTime());
        Map<String, PropertyValue> filter = ImmutableMap.of(ORDERED_PROPERTY,
            PropertyValues.newDate(searchFor));
        Iterator<? extends ResultRow> results = executeQuery(
            String.format(query, ORDERED_PROPERTY, ORDERED_PROPERTY), SQL2, filter).getRows()
            .iterator();
        Iterator<ValuePathTuple> filtered = Iterables.filter(nodes,
            new ValuePathTuple.GreaterThanPredicate(searchFor)).iterator();
        assertRightOrder(Lists.newArrayList(filtered), results);
        assertFalse("We should have looped throuhg all the results", results.hasNext());

        setTravesalEnabled(true);
    }

    /**
     * test the range query in case of '>=' condition
     * @throws CommitFailedException
     * @throws ParseException
     */
    @Test
    public void queryGreaterEqualThan() throws CommitFailedException, ParseException {
        setTravesalEnabled(false);

        final OrderDirection direction = OrderDirection.ASC;
        final String query = "SELECT * FROM [nt:base] AS n WHERE n.%s >= $%s";

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initialising the data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        Calendar start = midnightFirstJan2013();
        List<ValuePathTuple> nodes = addChildNodes(
            generateOrderedDates(NUMBER_OF_NODES, direction, start), test, direction, Type.DATE);
        root.commit();

        Calendar searchForCalendar = (Calendar) start.clone();
        searchForCalendar.add(Calendar.HOUR_OF_DAY, 36);
        String searchFor = ISO_8601_2000.format(searchForCalendar.getTime());
        Map<String, PropertyValue> filter = ImmutableMap.of(ORDERED_PROPERTY,
            PropertyValues.newDate(searchFor));
        Iterator<? extends ResultRow> results = executeQuery(
            String.format(query, ORDERED_PROPERTY, ORDERED_PROPERTY), SQL2, filter).getRows()
            .iterator();
        Iterator<ValuePathTuple> filtered = Iterables.filter(nodes,
                new ValuePathTuple.GreaterThanPredicate(searchFor, true))
                .iterator();
        assertRightOrder(Lists.newArrayList(filtered), results);
        assertFalse("We should have looped throuhg all the results", results.hasNext());

        setTravesalEnabled(true);
    }

    /**
     * test the range query in case of '<' condition
     *
     * in this case as we're ascending we're expecting an empty resultset with the proper
     * provider. not the lowcost one.
     * @throws Exception
     */
    @Test
    public void queryLessThan() throws Exception {
        initWithProperProvider();
        setTravesalEnabled(false);
        final OrderDirection direction = OrderDirection.DESC;
        final String query = "SELECT * FROM [nt:base] AS n WHERE n.%s < $%s";

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initialising the data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        Calendar start = midnightFirstJan2013();
        addChildNodes(
                generateOrderedDates(NUMBER_OF_NODES, direction, start), test, direction, Type.DATE);
        root.commit();

        Calendar searchForCalendar = (Calendar) start.clone();
        searchForCalendar.add(Calendar.HOUR_OF_DAY, -36);
        String searchFor = ISO_8601_2000.format(searchForCalendar.getTime());
        Map<String, PropertyValue> filter = ImmutableMap.of(ORDERED_PROPERTY,
                PropertyValues.newDate(searchFor));
        Iterator<? extends ResultRow> results = executeQuery(
            String.format(query, ORDERED_PROPERTY, ORDERED_PROPERTY), SQL2, filter).getRows()
            .iterator();
        assertFalse("We should have no results as of the cost and index direction", results.hasNext());

        setTravesalEnabled(true);
    }

    /**
     * test the range query in case of '<=' condition
     *
     * in this case as we're ascending we're expecting an empty resultset with the proper
     * provider. not the lowcost one.
     * @throws Exception
     */
    @Test
    public void queryLessEqualThan() throws Exception {
        initWithProperProvider();
        initWithProperProvider();
        setTravesalEnabled(false);
        final OrderDirection direction = OrderDirection.DESC;
        final String query = "SELECT * FROM [nt:base] AS n WHERE n.%s <= $%s";

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initialising the data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        Calendar start = midnightFirstJan2013();
        addChildNodes(
            generateOrderedDates(NUMBER_OF_NODES, direction, start), test, direction, Type.DATE);
        root.commit();

        Calendar searchForCalendar = (Calendar) start.clone();
        searchForCalendar.add(Calendar.HOUR_OF_DAY, -36);
        String searchFor = ISO_8601_2000.format(searchForCalendar.getTime());
        Map<String, PropertyValue> filter = ImmutableMap.of(ORDERED_PROPERTY,
            PropertyValues.newDate(searchFor));
        Iterator<? extends ResultRow> results = executeQuery(
            String.format(query, ORDERED_PROPERTY, ORDERED_PROPERTY), SQL2, filter).getRows()
            .iterator();
        assertFalse("We should have no results as of the cost and index direction", results.hasNext());

        setTravesalEnabled(true);
    }

    @Test
    public void queryAllEntriesWithOrderBy() throws CommitFailedException, ParseException, RepositoryException {
        setTravesalEnabled(false);

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        List<ValuePathTuple> nodes = addChildNodes(generateOrderedValues(NUMBER_OF_NODES), test,
            OrderDirection.ASC, Type.STRING);
        root.commit();

        // querying
        Iterator<? extends ResultRow> results;
        String query = String.format(
                "SELECT * from [nt:base] WHERE %s IS NOT NULL ORDER BY %s",
                ORDERED_PROPERTY,
                ORDERED_PROPERTY);
        results = executeQuery(query, SQL2, null)
            .getRows().iterator();
        assertRightOrder(nodes, results);

        setTravesalEnabled(true);
    }

    @Test @Ignore("OAK-1763")  // FIXME OAK-1763
    public void orderByOnDouble() throws CommitFailedException, ParseException, RepositoryException {
        setTravesalEnabled(false);

        Tree test = root.getTree("/").addChild("test");

        double v1 = 1.0E18;
        double v2 = 2.0E17;
        assertTrue(v2 <= v1);  // To be super sure ;-)

        Tree child1 = test.addChild(String.valueOf(v1));
        child1.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
        child1.setProperty(ORDERED_PROPERTY, v1, Type.DOUBLE);

        Tree child2 = test.addChild(String.valueOf(v2));
        child2.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
        child2.setProperty(ORDERED_PROPERTY, v2, Type.DOUBLE);

        root.commit();

        String query = String.format(
                "SELECT * from [nt:base] WHERE %s IS NOT NULL ORDER BY %s",
                ORDERED_PROPERTY,
                ORDERED_PROPERTY);
        Iterator<? extends ResultRow> results = executeQuery(query, SQL2, null).getRows().iterator();

        assertTrue(results.hasNext());
        double r1 = Double.valueOf(PathUtils.getName(results.next().getPath()));
        assertTrue(results.hasNext());
        double r2 = Double.valueOf(PathUtils.getName(results.next().getPath()));
        assertTrue(r1 <= r2);

        setTravesalEnabled(true);
    }

    @Test
    public void orderByQueryNoWhere() throws CommitFailedException, ParseException {
        setTravesalEnabled(false);

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        List<ValuePathTuple> nodes = addChildNodes(generateOrderedValues(NUMBER_OF_NODES), test,
            OrderDirection.ASC, Type.STRING);
        root.commit();

        // querying
        Iterator<? extends ResultRow> results;
        String query = String.format(
            "SELECT * from [nt:base] ORDER BY %s",
            ORDERED_PROPERTY);
        results = executeQuery(query, SQL2, null)
            .getRows().iterator();
        assertRightOrder(nodes, results);

        setTravesalEnabled(true);
    }

    @Test @Ignore("OAK-1763")  // FIXME OAK-1763
    public void orderByQueryOnSpecialChars() throws CommitFailedException, ParseException {
        setTravesalEnabled(false);

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        List<String> values = Lists.newArrayList("%", " ");
        List<ValuePathTuple> nodes = addChildNodes(values, test,
                OrderDirection.ASC, Type.STRING);
        root.commit();

        // querying
        Iterator<? extends ResultRow> results;
        String query = String.format(
                "SELECT * from [nt:base] ORDER BY %s",
                ORDERED_PROPERTY);
        results = executeQuery(query, SQL2, null)
                .getRows().iterator();
        assertRightOrder(nodes, results);

        setTravesalEnabled(true);
    }

    @Test
    public void planOderByNoWhere() throws IllegalArgumentException, RepositoryException,
                                   CommitFailedException {

        NodeBuilder root = EmptyNodeState.EMPTY_NODE.builder();

        IndexUtils.createIndexDefinition(root.child(IndexConstants.INDEX_DEFINITIONS_NAME),
            TEST_INDEX_NAME, false, ImmutableList.of(ORDERED_PROPERTY), null, OrderedIndex.TYPE,
            ImmutableMap.<String, String> of());

        NodeState before = root.getNodeState();
        final OrderDirection direction = OrderDirection.ASC;
        final QueryIndex.OrderEntry.Order order = OrderDirection.ASC.equals(direction) ? QueryIndex.OrderEntry.Order.ASCENDING
                                                                                      : QueryIndex.OrderEntry.Order.DESCENDING;

        List<String> values = generateOrderedValues(NUMBER_OF_NODES, direction);
        addChildNodes(values, root, Type.STRING);
        NodeState after = root.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        final OrderedPropertyIndex index = new OrderedPropertyIndex();
        final String nodeTypeName = JcrConstants.NT_BASE;

        Filter filter = createFilter(indexed, nodeTypeName);

        List<QueryIndex.OrderEntry> sortOrder = ImmutableList.of(createOrderEntry(ORDERED_PROPERTY,
            order));
        List<IndexPlan> plans = index.getPlans(filter, sortOrder, indexed);

        assertNotNull(plans);
        assertEquals(1, plans.size());
        IndexPlan p = plans.get(0);
        assertTrue(p.getEstimatedEntryCount() > 0);
        assertNotNull(p.getSortOrder());
        assertEquals(1, p.getSortOrder().size());
        QueryIndex.OrderEntry oe = p.getSortOrder().get(0);
        assertNotNull(oe);
        assertEquals(ORDERED_PROPERTY, oe.getPropertyName());
        assertEquals(QueryIndex.OrderEntry.Order.ASCENDING, oe.getOrder());
    }

    @Test
    public void queryOrderByNonIndexedProperty() throws CommitFailedException, ParseException {
        setTravesalEnabled(false);

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        addChildNodes(generateOrderedValues(NUMBER_OF_NODES), test,
            OrderDirection.ASC, Type.STRING);
        root.commit();

        // querying
        Iterator<? extends ResultRow> results;
        String query = "SELECT * from [nt:base] ORDER BY somethingnotindexed";
        results = executeQuery(query, SQL2, null)
            .getRows().iterator();
        assertFalse("An empty resultset is expected", results.hasNext());

        setTravesalEnabled(true);
    }

    private static FilterImpl createFilter(NodeState indexed, String nodeTypeName) {
        NodeState system = indexed.getChildNode(JCR_SYSTEM);
        NodeState types = system.getChildNode(JCR_NODE_TYPES);
        NodeState type = types.getChildNode(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    private static QueryIndex.OrderEntry createOrderEntry(String property,
                                                          QueryIndex.OrderEntry.Order order) {
        return new QueryIndex.OrderEntry(property, Type.UNDEFINED, order);
    }

    @Test
    public void planOrderByNonIndexedProperty() throws IllegalArgumentException,
                                               RepositoryException, CommitFailedException {
        NodeBuilder root = EmptyNodeState.EMPTY_NODE.builder();

        IndexUtils.createIndexDefinition(root.child(IndexConstants.INDEX_DEFINITIONS_NAME),
            TEST_INDEX_NAME, false, ImmutableList.of(ORDERED_PROPERTY), null, OrderedIndex.TYPE,
            ImmutableMap.<String, String> of());

        NodeState before = root.getNodeState();
        final OrderDirection direction = OrderDirection.ASC;
        final QueryIndex.OrderEntry.Order order = OrderDirection.ASC.equals(direction) ? QueryIndex.OrderEntry.Order.ASCENDING
                                                                                      : QueryIndex.OrderEntry.Order.DESCENDING;
        List<String> values = generateOrderedValues(NUMBER_OF_NODES, direction);
        addChildNodes(values, root, Type.STRING);
        NodeState after = root.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        final OrderedPropertyIndex index = new OrderedPropertyIndex();
        final String nodeTypeName = JcrConstants.NT_BASE;
        Filter filter = createFilter(indexed, nodeTypeName);

        List<QueryIndex.OrderEntry> sortOrder = ImmutableList.of(createOrderEntry(
            "somethingnotindexed", order));

        List<IndexPlan> plans = index.getPlans(filter, sortOrder, indexed);

        assertNotNull(plans);
        assertEquals(0, plans.size());
    }

    /**
     * tests the output of a plan where the query is asked with where conditions that are not indexed
     * but the ORDER BY are on the indexed property
     *
     * eg: SELECT * FROM [nt:base] WHERE pinned=1 ORDER BY lastModified
     *
     * @throws RepositoryException
     * @throws IllegalArgumentException
     * @throws CommitFailedException
     */
    @Test
    public void planOrderAndWhereMixed() throws IllegalArgumentException, RepositoryException, CommitFailedException {
        NodeBuilder root = EmptyNodeState.EMPTY_NODE.builder();

        IndexUtils.createIndexDefinition(root.child(IndexConstants.INDEX_DEFINITIONS_NAME),
            TEST_INDEX_NAME, false, ImmutableList.of(ORDERED_PROPERTY), null, OrderedIndex.TYPE,
            ImmutableMap.<String, String> of());

        NodeState before = root.getNodeState();
        final OrderDirection direction = OrderDirection.ASC;
        final QueryIndex.OrderEntry.Order order = OrderDirection.ASC.equals(direction) ? QueryIndex.OrderEntry.Order.ASCENDING
                                                                                      : QueryIndex.OrderEntry.Order.DESCENDING;
        List<String> values = generateOrderedValues(NUMBER_OF_NODES, direction);
        addChildNodes(values, root, Type.STRING);
        NodeState after = root.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        final OrderedPropertyIndex index = new OrderedPropertyIndex();
        final String nodeTypeName = JcrConstants.NT_BASE;
        FilterImpl filter = createFilter(indexed, nodeTypeName);
        filter.restrictProperty("somethingNotIndexed", Operator.EQUAL, PropertyValues.newLong(1L));

        List<QueryIndex.OrderEntry> sortOrder = ImmutableList.of(createOrderEntry(
            ORDERED_PROPERTY, order));

        List<IndexPlan> plans = index.getPlans(filter, sortOrder, indexed);
        assertNotNull(plans);
        assertEquals(1, plans.size());
        IndexPlan p = plans.get(0);
        assertTrue(p.getEstimatedEntryCount() > 0);
        assertNotNull(p.getSortOrder());
        assertEquals(1, p.getSortOrder().size());
        assertEquals(QueryIndex.OrderEntry.Order.ASCENDING, p.getSortOrder()
                .get(0).getOrder());
    }

    /**
     * query the index in case of mixed situation
     *
     * eg: SELECT * FROM [nt:base] WHERE pinned=1 ORDER BY lastModified
     *
     * @throws RepositoryException
     * @throws IllegalArgumentException
     * @throws CommitFailedException
     * @throws ParseException
     */
    @Test
    public void queryOrderAndWhereMixed() throws IllegalArgumentException, RepositoryException, CommitFailedException, ParseException {
        setTravesalEnabled(false);

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initiate the repo with some data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        List<ValuePathTuple> nodes = addChildNodes(generateOrderedValues(NUMBER_OF_NODES), test,
            OrderDirection.ASC, Type.STRING);
        root.commit();

        String where = "wholetthedogsout";
        String value = "woof-woof-woof-woof";

        // let's set the property that will have to be queried only on 2 nodes
        Tree t = root.getTree(nodes.get(0).getPath());
        t.setProperty(where, value);
        t = root.getTree(nodes.get(1).getPath());
        t.setProperty(where, value);
        root.commit();

        // querying
        Iterator<? extends ResultRow> results;
        String query = String.format(
            "SELECT * from [nt:base] WHERE %s=$%s ORDER BY %s",
            where,
            where,
            ORDERED_PROPERTY
            );
        Map<String, PropertyValue> filter = ImmutableMap.of(
            where, PropertyValues.newString(value)
            );
        results = executeQuery(query, SQL2, filter)
            .getRows().iterator();
        assertTrue(results.hasNext());

        setTravesalEnabled(true);
    }

    /**
     * testing explicitly OAK-1561 use-case
     * 
     * @throws CommitFailedException
     * @throws ParseException
     */
    @Test
    public void queryGreaterThenWithCast() throws CommitFailedException, ParseException {

        setTravesalEnabled(false);

        final OrderDirection direction = OrderDirection.ASC;
        final String query = "SELECT * FROM [nt:base] WHERE " + ORDERED_PROPERTY
                             + "> cast('%s' as date)";

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initialising the data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        Calendar start = midnightFirstJan2013();
        List<ValuePathTuple> nodes = addChildNodes(
            generateOrderedDates(NUMBER_OF_NODES, direction, start), test, direction, Type.DATE);
        root.commit();

        Calendar searchForCalendar = (Calendar) start.clone();
        searchForCalendar.add(Calendar.HOUR_OF_DAY, 36);
        String searchFor = ISO_8601_2000.format(searchForCalendar.getTime());
        Iterator<? extends ResultRow> results = executeQuery(String.format(query, searchFor), SQL2,
            null).getRows().iterator();
        Iterator<ValuePathTuple> filtered = Iterables.filter(nodes,
            new ValuePathTuple.GreaterThanPredicate(searchFor)).iterator();
        assertRightOrder(Lists.newArrayList(filtered), results);
        assertFalse("We should have looped throuhg all the results", results.hasNext());

        setTravesalEnabled(true);

    }

    @Test
    public void queryBetweenNoIncludes() throws Exception {
        setTravesalEnabled(false);

        final OrderDirection direction = OrderDirection.ASC;
        final String query = "SELECT * FROM [nt:base] WHERE " + ORDERED_PROPERTY + "> $start AND "
                             + ORDERED_PROPERTY + " < $end";

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initialising the data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        Calendar start = midnightFirstJan2013();

        List<ValuePathTuple> nodes = addChildNodes(
            generateOrderedDates(NUMBER_OF_NODES, direction, start), test, direction, Type.DATE);
        root.commit();

        Calendar searchForCalendarStart = (Calendar) start.clone();
        searchForCalendarStart.add(Calendar.HOUR_OF_DAY, 36);
        String searchForStart = ISO_8601_2000.format(searchForCalendarStart.getTime());

        Calendar endCalendar = Calendar.getInstance();
        endCalendar.setTime(ISO_8601_2000.parse(nodes.get(nodes.size() - 1).getValue()));
        endCalendar.add(Calendar.HOUR_OF_DAY, -36);
        String searchForEnd = ISO_8601_2000.format(endCalendar.getTime());

        Map<String, PropertyValue> filter = ImmutableMap.of("start",
            PropertyValues.newDate(searchForStart), "end", PropertyValues.newDate(searchForEnd));
        Iterator<? extends ResultRow> results = executeQuery(query, SQL2, filter).getRows()
            .iterator();

        Iterator<ValuePathTuple> filtered = Iterables.filter(nodes,
            new ValuePathTuple.BetweenPredicate(searchForStart, searchForEnd, false, false))
            .iterator();

        assertRightOrder(Lists.newArrayList(filtered), results);
        assertFalse("We should have looped throuhg all the results", results.hasNext());

        setTravesalEnabled(true);

    }

    @Test
    public void queryBetweenIncludeLower() throws Exception {
        setTravesalEnabled(false);

        final OrderDirection direction = OrderDirection.ASC;
        final String query = "SELECT * FROM [nt:base] WHERE " + ORDERED_PROPERTY + ">= $start AND "
                             + ORDERED_PROPERTY + " < $end";

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initialising the data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        Calendar start = midnightFirstJan2013();

        List<ValuePathTuple> nodes = addChildNodes(
            generateOrderedDates(NUMBER_OF_NODES, direction, start), test, direction, Type.DATE);
        root.commit();

        Calendar searchForCalendarStart = (Calendar) start.clone();
        searchForCalendarStart.add(Calendar.HOUR_OF_DAY, 36);
        String searchForStart = ISO_8601_2000.format(searchForCalendarStart.getTime());

        Calendar endCalendar = Calendar.getInstance();
        endCalendar.setTime(ISO_8601_2000.parse(nodes.get(nodes.size() - 1).getValue()));
        endCalendar.add(Calendar.HOUR_OF_DAY, -36);
        String searchForEnd = ISO_8601_2000.format(endCalendar.getTime());

        Map<String, PropertyValue> filter = ImmutableMap.of("start",
            PropertyValues.newDate(searchForStart), "end", PropertyValues.newDate(searchForEnd));
        Iterator<? extends ResultRow> results = executeQuery(query, SQL2, filter).getRows()
            .iterator();

        Iterator<ValuePathTuple> filtered = Iterables.filter(nodes,
            new ValuePathTuple.BetweenPredicate(searchForStart, searchForEnd, true, false))
            .iterator();

        assertRightOrder(Lists.newArrayList(filtered), results);
        assertFalse("We should have looped throuhg all the results", results.hasNext());

        setTravesalEnabled(true);

    }
    
    @Test
    public void queryBetweenIncludeHigher() throws Exception {
        setTravesalEnabled(false);

        final OrderDirection direction = OrderDirection.ASC;
        final String query = "SELECT * FROM [nt:base] WHERE " + ORDERED_PROPERTY + "> $start AND "
                             + ORDERED_PROPERTY + " <= $end";

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initialising the data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        Calendar start = midnightFirstJan2013();

        List<ValuePathTuple> nodes = addChildNodes(
            generateOrderedDates(10, direction, start), test, direction, Type.DATE);
        root.commit();
        
        Calendar searchForCalendarStart = (Calendar) start.clone();
        searchForCalendarStart.add(Calendar.HOUR_OF_DAY, 36);
        String searchForStart = ISO_8601_2000.format(searchForCalendarStart.getTime());

        Calendar endCalendar = Calendar.getInstance();
        endCalendar.setTime(ISO_8601_2000.parse(nodes.get(nodes.size() - 1).getValue()));
        endCalendar.add(Calendar.HOUR_OF_DAY, -36);
        String searchForEnd = ISO_8601_2000.format(endCalendar.getTime());

        Map<String, PropertyValue> filter = ImmutableMap.of("start",
            PropertyValues.newDate(searchForStart), "end", PropertyValues.newDate(searchForEnd));
        Iterator<? extends ResultRow> results = executeQuery(query, SQL2, filter).getRows()
            .iterator();

        Iterator<ValuePathTuple> filtered = Iterables.filter(nodes,
            new ValuePathTuple.BetweenPredicate(searchForStart, searchForEnd, false, true))
            .iterator();
        
        assertRightOrder(Lists.newArrayList(filtered), results);
        assertFalse("We should have looped throuhg all the results", results.hasNext());

        setTravesalEnabled(true);

    }

    @Test
    public void queryBetweenIncludeBoth() throws Exception {
        setTravesalEnabled(false);

        final OrderDirection direction = OrderDirection.ASC;
        final String query = "SELECT * FROM [nt:base] WHERE " + ORDERED_PROPERTY + ">= $start AND "
                             + ORDERED_PROPERTY + " <= $end";

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initialising the data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        Calendar start = midnightFirstJan2013();

        List<ValuePathTuple> nodes = addChildNodes(
            generateOrderedDates(10, direction, start), test, direction, Type.DATE);
        root.commit();

        Calendar searchForCalendarStart = (Calendar) start.clone();
        searchForCalendarStart.add(Calendar.HOUR_OF_DAY, 36);
        String searchForStart = ISO_8601_2000.format(searchForCalendarStart.getTime());

        Calendar endCalendar = Calendar.getInstance();
        endCalendar.setTime(ISO_8601_2000.parse(nodes.get(nodes.size() - 1).getValue()));
        endCalendar.add(Calendar.HOUR_OF_DAY, -36);
        String searchForEnd = ISO_8601_2000.format(endCalendar.getTime());

        Map<String, PropertyValue> filter = ImmutableMap.of("start",
            PropertyValues.newDate(searchForStart), "end", PropertyValues.newDate(searchForEnd));
        Iterator<? extends ResultRow> results = executeQuery(query, SQL2, filter).getRows()
            .iterator();

        Iterator<ValuePathTuple> filtered = Iterables.filter(nodes,
            new ValuePathTuple.BetweenPredicate(searchForStart, searchForEnd, true, true))
            .iterator();
        
        assertRightOrder(Lists.newArrayList(filtered), results);
        assertFalse("We should have looped throuhg all the results", results.hasNext());

        setTravesalEnabled(true);

    }
}