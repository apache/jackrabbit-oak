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
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;


public class OrderedPropertyIndexDescendingQueryTest extends BasicOrderedPropertyIndexQueryTest {
    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        IndexUtils.createIndexDefinition(
            new NodeUtil(index.getChild(IndexConstants.INDEX_DEFINITIONS_NAME)),
            TEST_INDEX_NAME,
            false,
            new String[] { ORDERED_PROPERTY },
            null,
            OrderedIndex.TYPE,
            ImmutableMap.of(
                OrderedIndex.DIRECTION, OrderedIndex.OrderDirection.DESC.getDirection()
            )
        );
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
        List<ValuePathTuple> nodes = addChildNodes(
            generateOrderedValues(NUMBER_OF_NODES, OrderDirection.DESC), test, OrderDirection.DESC,
            Type.STRING);
        root.commit();

        // querying
        Iterator<? extends ResultRow> results;
        results = executeQuery(
            String.format("SELECT * from [%s] WHERE foo IS NOT NULL", NT_UNSTRUCTURED), SQL2, null)
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
            OrderDirection.DESC, Type.STRING);
        root.commit();

        // getting the middle of the random list of nodes
        ValuePathTuple searchfor = nodes.get(NUMBER_OF_NODES / 2);

        Map<String, PropertyValue> filter = ImmutableMap.of(ORDERED_PROPERTY,
            PropertyValues.newString(searchfor.getValue()));
        String query = "SELECT * FROM [%s] WHERE %s=$%s";
        Iterator<? extends ResultRow> results = executeQuery(
            String.format(query, NT_UNSTRUCTURED, ORDERED_PROPERTY, ORDERED_PROPERTY),
            SQL2,
            filter).getRows().iterator();
        assertTrue("one element is expected", results.hasNext());
        assertEquals("wrong path returned", searchfor.getPath(), results.next().getPath());
        assertFalse("there should be not any more items", results.hasNext());

        setTravesalEnabled(true);
    }

    /**
     * test the range query in case of '>' condition
     * @throws Exception
     */
    @Test @Ignore("Disabling for now. Integration with OAK-622 and prioritising.")
    public void queryGreaterThan() throws Exception {
        initWithProperProvider();
        setTravesalEnabled(false);

        final OrderDirection direction = OrderDirection.ASC;
        final String query = "SELECT * FROM [nt:base] AS n WHERE n.%s > $%s";

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
        searchForCalendar.add(Calendar.HOUR_OF_DAY, 36);
        String searchFor = ISO_8601_2000.format(searchForCalendar.getTime());
        Map<String, PropertyValue> filter = ImmutableMap.of(ORDERED_PROPERTY,
            PropertyValues.newDate(searchFor));
        Iterator<? extends ResultRow> results = executeQuery(
            String.format(query, ORDERED_PROPERTY, ORDERED_PROPERTY), SQL2, filter).getRows()
            .iterator();
        assertFalse("We should not return any results as of the cost", results.hasNext());

        setTravesalEnabled(true);
    }

    /**
     * test the range query in case of '>=' condition
     * @throws Exception
     */
    @Test @Ignore("Disabling for now. Integration with OAK-622 and prioritising.")
    public void queryGreaterEqualThan() throws Exception {
        initWithProperProvider();
        setTravesalEnabled(false);

        final OrderDirection direction = OrderDirection.ASC;
        final String query = "SELECT * FROM [nt:base] AS n WHERE n.%s >= $%s";

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
        searchForCalendar.add(Calendar.HOUR_OF_DAY, 36);
        String searchFor = ISO_8601_2000.format(searchForCalendar.getTime());
        Map<String, PropertyValue> filter = ImmutableMap.of(ORDERED_PROPERTY,
            PropertyValues.newDate(searchFor));
        Iterator<? extends ResultRow> results = executeQuery(
            String.format(query, ORDERED_PROPERTY, ORDERED_PROPERTY), SQL2, filter).getRows()
            .iterator();
        assertFalse("We should not return any results as of the cost", results.hasNext());

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
        setTravesalEnabled(false);
        final OrderDirection direction = OrderDirection.DESC;
        final String query = "SELECT * FROM [nt:base] AS n WHERE n.%s < $%s";

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initialising the data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        Calendar start = midnightFirstJan2013();
        List<ValuePathTuple> nodes = addChildNodes(
            generateOrderedDates(10, direction, start), test, direction, Type.DATE);
        root.commit();

        Calendar searchForCalendar = (Calendar) start.clone();
        searchForCalendar.add(Calendar.HOUR_OF_DAY, -36);
        String searchFor = ISO_8601_2000.format(searchForCalendar.getTime());
        Map<String, PropertyValue> filter = ImmutableMap.of(ORDERED_PROPERTY,
            PropertyValues.newDate(searchFor));
        Iterator<? extends ResultRow> results = executeQuery(
            String.format(query, ORDERED_PROPERTY, ORDERED_PROPERTY), SQL2, filter).getRows()
            .iterator();
        Iterator<ValuePathTuple> filtered = Iterables.filter(nodes,
            new ValuePathTuple.LessThanPredicate(searchFor)).iterator();
        assertRightOrder(Lists.newArrayList(filtered), results);
        assertFalse("no more results expected", results.hasNext());

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
        setTravesalEnabled(false);
        final OrderDirection direction = OrderDirection.DESC;
        final String query = "SELECT * FROM [nt:base] AS n WHERE n.%s <= $%s";

        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        // initialising the data
        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        Calendar start = midnightFirstJan2013();
        List<ValuePathTuple> nodes = addChildNodes(
            generateOrderedDates(10, direction, start), test, direction, Type.DATE);
        root.commit();

        Calendar searchForCalendar = (Calendar) start.clone();
        searchForCalendar.add(Calendar.HOUR_OF_DAY, -36);
        String searchFor = ISO_8601_2000.format(searchForCalendar.getTime());
        Map<String, PropertyValue> filter = ImmutableMap.of(ORDERED_PROPERTY,
            PropertyValues.newDate(searchFor));
        Iterator<? extends ResultRow> results = executeQuery(
            String.format(query, ORDERED_PROPERTY, ORDERED_PROPERTY), SQL2, filter).getRows()
            .iterator();
        Iterator<ValuePathTuple> filtered = Iterables.filter(nodes,
                new ValuePathTuple.LessThanPredicate(searchFor, true))
                .iterator();
        assertRightOrder(Lists.newArrayList(filtered), results);
        assertFalse("no more results expecrted", results.hasNext());

        setTravesalEnabled(true);
    }

}
