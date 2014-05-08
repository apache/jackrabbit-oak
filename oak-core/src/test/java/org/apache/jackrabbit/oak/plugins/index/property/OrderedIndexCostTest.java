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

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.easymock.EasyMock;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * tests the Cost-related part of the provider/strategy
 */
public class OrderedIndexCostTest extends BasicOrderedPropertyIndexQueryTest {
    /**
     * convenience class that return an always indexed strategy
     */
    private static class AlwaysIndexedOrderedPropertyIndex extends OrderedPropertyIndex {
        @Override
        PropertyIndexLookup getLookup(NodeState root) {
            return new AlwaysIndexedLookup(root);
        }

        /**
         * convenience class that always return true at the isIndexed test
         */
        private static class AlwaysIndexedLookup extends OrderedPropertyIndexLookup {
            public AlwaysIndexedLookup(NodeState root) {
                super(root);
            }

            @Override
            public boolean isIndexed(String propertyName, String path, Filter filter) {
                return true;
            }
        }
      }

    @Override
    protected void createTestIndexNode() throws Exception {
        // intentionally left blank. Each test will have to define its own index configuration
    }

    private static void defineIndex(NodeBuilder root, OrderDirection direction) throws IllegalArgumentException, RepositoryException {
        IndexUtils
            .createIndexDefinition(
                root.child(IndexConstants.INDEX_DEFINITIONS_NAME),
                TEST_INDEX_NAME,
                false,
                ImmutableList.of(ORDERED_PROPERTY),
                null,
                OrderedIndex.TYPE,
                ImmutableMap.of(OrderedIndex.DIRECTION,
                    direction.getDirection()));
        // forcing the existence of :index
        root.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME).getChildNode(TEST_INDEX_NAME)
            .child(IndexConstants.INDEX_CONTENT_NODE_NAME);
    }

    /**
     * define e descending ordered index in the provided root
     *
     * @param root
     * @throws IllegalArgumentException
     * @throws RepositoryException
     */
    private static void defineDescendingIndex(NodeBuilder root) throws IllegalArgumentException,
                                                               RepositoryException {
        defineIndex(root, OrderDirection.DESC);
    }

    /**
     * define e Ascending ordered index in the provided root
     *
     * @param root
     * @throws IllegalArgumentException
     * @throws RepositoryException
     */
    private static void defineAscendingIndex(NodeBuilder root) throws IllegalArgumentException,
                                                               RepositoryException {
        defineIndex(root, OrderDirection.ASC);
    }

    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costFullTextConstraint() {
        OrderedPropertyIndex index = new OrderedPropertyIndex();
        NodeState root = InitialContent.INITIAL_CONTENT;
        Filter filter = EasyMock.createNiceMock(Filter.class);
        FullTextExpression fte = EasyMock.createNiceMock(FullTextExpression.class);
        EasyMock.expect(filter.getFullTextConstraint()).andReturn(fte).anyTimes();
        EasyMock.replay(fte);
        EasyMock.replay(filter);

        assertEquals("if it contains FullText we don't serve", Double.POSITIVE_INFINITY,
            index.getCost(filter, root), 0);
    }

    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costContainsNativeConstraints(){
        OrderedPropertyIndex index = new OrderedPropertyIndex();
        NodeState root = InitialContent.INITIAL_CONTENT;
        Filter filter = EasyMock.createNiceMock(Filter.class);
        EasyMock.expect(filter.containsNativeConstraint()).andReturn(true).anyTimes();
        EasyMock.replay(filter);

        assertEquals("If it contains Natives we don't serve", Double.POSITIVE_INFINITY,
            index.getCost(filter, root), 0);
    }

    /**
     * tests the use-case where we ask for '>' of a date.
     *
     * As we're not testing the actual algorithm, part of {@code IndexLookup} we want to make sure
     * the Index doesn't reply with "dont' serve" in special cases
     */
    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costGreaterThanAscendingDirection() throws Exception {
        OrderedPropertyIndex index = new AlwaysIndexedOrderedPropertyIndex();
        NodeBuilder builder = InitialContent.INITIAL_CONTENT.builder();
        defineAscendingIndex(builder);
        NodeState root = builder.getNodeState();
        Filter filter = createNiceMock(Filter.class);
        Filter.PropertyRestriction restriction = new Filter.PropertyRestriction();
        restriction.first = PropertyValues.newDate("2013-01-01");
        restriction.propertyName = ORDERED_PROPERTY;
        expect(filter.getPropertyRestrictions()).andReturn(ImmutableList.of(restriction))
            .anyTimes();
        expect(filter.containsNativeConstraint()).andReturn(false).anyTimes();
        replay(filter);

        assertFalse("In ascending order we're expeting to serve this kind of queries",
            Double.POSITIVE_INFINITY == index.getCost(filter, root));
    }

    /**
     * test that the '>=' use case is served from the index
     * @throws RepositoryException
     * @throws IllegalArgumentException
     */
    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costGreaterThanEqualAscendingDirection() throws IllegalArgumentException, RepositoryException {
        OrderedPropertyIndex index = new AlwaysIndexedOrderedPropertyIndex();
        NodeBuilder builder = InitialContent.INITIAL_CONTENT.builder();
        defineAscendingIndex(builder);
        NodeState root = builder.getNodeState();
        Filter filter = createNiceMock(Filter.class);
        Filter.PropertyRestriction restriction = new Filter.PropertyRestriction();
        restriction.first = PropertyValues.newDate("2013-01-01");
        restriction.firstIncluding = true;
        restriction.propertyName = ORDERED_PROPERTY;
        expect(filter.getPropertyRestrictions()).andReturn(ImmutableList.of(restriction))
            .anyTimes();
        expect(filter.containsNativeConstraint()).andReturn(false).anyTimes();
        replay(filter);

        assertFalse("In ascending order we're expeting to serve this kind of queries",
            Double.POSITIVE_INFINITY == index.getCost(filter, root));
    }

    /**
     * when we run a '<' in an Ascending index it should not serve it
     * @throws RepositoryException
     * @throws IllegalArgumentException
     */
    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costLessThanAscendingDirection() throws IllegalArgumentException, RepositoryException {
        OrderedPropertyIndex index = new AlwaysIndexedOrderedPropertyIndex();
        NodeBuilder builder = InitialContent.INITIAL_CONTENT.builder();
        defineAscendingIndex(builder);
        NodeState root = builder.getNodeState();
        Filter filter = createNiceMock(Filter.class);
        Filter.PropertyRestriction restriction = new Filter.PropertyRestriction();
        restriction.last = PropertyValues.newDate("2013-01-01");
        restriction.propertyName = ORDERED_PROPERTY;
        expect(filter.getPropertyRestrictions()).andReturn(ImmutableList.of(restriction))
            .anyTimes();
        expect(filter.containsNativeConstraint()).andReturn(false).anyTimes();
        replay(filter);

        assertEquals("in ascending index we're not expecting to serve '<' queries",
            Double.POSITIVE_INFINITY, index.getCost(filter, root), 0);
    }

    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costLessThanEqualsAscendingDirection() throws IllegalArgumentException, RepositoryException {
        OrderedPropertyIndex index = new AlwaysIndexedOrderedPropertyIndex();
        NodeBuilder builder = InitialContent.INITIAL_CONTENT.builder();
        defineAscendingIndex(builder);
        NodeState root = builder.getNodeState();
        Filter filter = createNiceMock(Filter.class);
        Filter.PropertyRestriction restriction = new Filter.PropertyRestriction();
        restriction.last = PropertyValues.newDate("2013-01-01");
        restriction.lastIncluding = true;
        restriction.propertyName = ORDERED_PROPERTY;
        expect(filter.getPropertyRestrictions()).andReturn(ImmutableList.of(restriction))
            .anyTimes();
        expect(filter.containsNativeConstraint()).andReturn(false).anyTimes();
        replay(filter);

        assertEquals("in ascending index we're not expecting to serve '<=' queries",
            Double.POSITIVE_INFINITY, index.getCost(filter, root), 0);
    }

    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costGreaterThanDescendingDirection() throws IllegalArgumentException, RepositoryException {
        OrderedPropertyIndex index = new AlwaysIndexedOrderedPropertyIndex();
        NodeBuilder builder = InitialContent.INITIAL_CONTENT.builder();
        defineDescendingIndex(builder);
        NodeState root = builder.getNodeState();
        Filter filter = createNiceMock(Filter.class);
        Filter.PropertyRestriction restriction = new Filter.PropertyRestriction();
        restriction.first = PropertyValues.newDate("2013-01-01");
        restriction.propertyName = ORDERED_PROPERTY;
        expect(filter.getPropertyRestrictions()).andReturn(ImmutableList.of(restriction))
            .anyTimes();
        expect(filter.containsNativeConstraint()).andReturn(false).anyTimes();
        replay(filter);

        assertEquals("in descending index we're not expecting to serve '>' queries",
            Double.POSITIVE_INFINITY, index.getCost(filter, root), 0);

    }

    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costGreaterEqualThanDescendingDirection() throws IllegalArgumentException, RepositoryException {
        OrderedPropertyIndex index = new AlwaysIndexedOrderedPropertyIndex();
        NodeBuilder builder = InitialContent.INITIAL_CONTENT.builder();
        defineDescendingIndex(builder);
        NodeState root = builder.getNodeState();
        Filter filter = createNiceMock(Filter.class);
        Filter.PropertyRestriction restriction = new Filter.PropertyRestriction();
        restriction.first = PropertyValues.newDate("2013-01-01");
        restriction.firstIncluding = true;
        restriction.propertyName = ORDERED_PROPERTY;
        expect(filter.getPropertyRestrictions()).andReturn(ImmutableList.of(restriction))
            .anyTimes();
        expect(filter.containsNativeConstraint()).andReturn(false).anyTimes();
        replay(filter);

        assertEquals("in descending index we're not expecting to serve '>' queries",
            Double.POSITIVE_INFINITY, index.getCost(filter, root), 0);

    }

    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costLessThanDescendingDirection() throws IllegalArgumentException, RepositoryException {
        OrderedPropertyIndex index = new AlwaysIndexedOrderedPropertyIndex();
        NodeBuilder builder = InitialContent.INITIAL_CONTENT.builder();
        defineDescendingIndex(builder);
        NodeState root = builder.getNodeState();
        Filter filter = createNiceMock(Filter.class);
        Filter.PropertyRestriction restriction = new Filter.PropertyRestriction();
        restriction.last = PropertyValues.newDate("2013-01-01");
        restriction.propertyName = ORDERED_PROPERTY;
        expect(filter.getPropertyRestrictions()).andReturn(ImmutableList.of(restriction))
            .anyTimes();
        expect(filter.containsNativeConstraint()).andReturn(false).anyTimes();
        replay(filter);

        assertFalse("In descending order we're expeting to serve this kind of queries",
            Double.POSITIVE_INFINITY == index.getCost(filter, root));

    }

    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costLessThanEqualDescendingDirection() throws IllegalArgumentException, RepositoryException {
        OrderedPropertyIndex index = new AlwaysIndexedOrderedPropertyIndex();
        NodeBuilder builder = InitialContent.INITIAL_CONTENT.builder();
        defineDescendingIndex(builder);
        NodeState root = builder.getNodeState();
        Filter filter = createNiceMock(Filter.class);
        Filter.PropertyRestriction restriction = new Filter.PropertyRestriction();
        restriction.last = PropertyValues.newDate("2013-01-01");
        restriction.lastIncluding = true;
        restriction.propertyName = ORDERED_PROPERTY;
        expect(filter.getPropertyRestrictions()).andReturn(ImmutableList.of(restriction))
            .anyTimes();
        expect(filter.containsNativeConstraint()).andReturn(false).anyTimes();
        replay(filter);

        assertFalse("In descending order we're expeting to serve this kind of queries",
            Double.POSITIVE_INFINITY == index.getCost(filter, root));

    }

    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costBetweenDescendingDirection() throws IllegalArgumentException, RepositoryException {
        OrderedPropertyIndex index = new AlwaysIndexedOrderedPropertyIndex();
        NodeBuilder builder = InitialContent.INITIAL_CONTENT.builder();
        defineDescendingIndex(builder);
        NodeState root = builder.getNodeState();
        Filter filter = createNiceMock(Filter.class);
        Filter.PropertyRestriction restriction = new Filter.PropertyRestriction();
        restriction.first = PropertyValues.newDate("2013-01-01");
        restriction.last = PropertyValues.newDate("2013-01-02");
        restriction.firstIncluding = true;
        restriction.lastIncluding = true;
        restriction.propertyName = ORDERED_PROPERTY;
        expect(filter.getPropertyRestrictions()).andReturn(ImmutableList.of(restriction))
            .anyTimes();
        expect(filter.containsNativeConstraint()).andReturn(false).anyTimes();
        replay(filter);

        assertFalse("In descending order we're expeting to serve this kind of queries",
            Double.POSITIVE_INFINITY == index.getCost(filter, root));

    }

    @Test @Ignore("As of OAK-622 this should no longer be used. Removing later.")
    public void costBetweenAscendingDirection() throws IllegalArgumentException, RepositoryException {
        OrderedPropertyIndex index = new AlwaysIndexedOrderedPropertyIndex();
        NodeBuilder builder = InitialContent.INITIAL_CONTENT.builder();
        defineAscendingIndex(builder);
        NodeState root = builder.getNodeState();
        Filter filter = createNiceMock(Filter.class);
        Filter.PropertyRestriction restriction = new Filter.PropertyRestriction();
        restriction.first = PropertyValues.newDate("2013-01-01");
        restriction.last = PropertyValues.newDate("2013-01-02");
        restriction.firstIncluding = true;
        restriction.lastIncluding = true;
        restriction.propertyName = ORDERED_PROPERTY;
        expect(filter.getPropertyRestrictions()).andReturn(ImmutableList.of(restriction))
            .anyTimes();
        expect(filter.containsNativeConstraint()).andReturn(false).anyTimes();
        replay(filter);

        assertFalse("In descending order we're expeting to serve this kind of queries",
            Double.POSITIVE_INFINITY == index.getCost(filter, root));

    }
}
