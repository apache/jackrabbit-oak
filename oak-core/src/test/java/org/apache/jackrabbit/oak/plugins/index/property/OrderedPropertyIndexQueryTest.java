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
import static junit.framework.Assert.fail;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class OrderedPropertyIndexQueryTest extends AbstractQueryTest {
    /**
     * the property used by the index
     */
    public static final String ORDERED_PROPERTY = "foo";

    /**
     * number of nodes to create for testing.
     * 
     * It has been found during development that in some cases the order of the nodes creation within the persistence
     * where the actual expected order.
     * 
     * The higher the value the lower the chance for this to happen.
     */
    private static final int NUMBER_OF_NODES = 50;

    /**
     * convenience orderable object that represents a tuple of values and paths
     * 
     * where the values are the indexed keys from the index and the paths are the path which hold the key
     */
    private class ValuePathTuple implements Comparable<ValuePathTuple> {
        private final String value;
        private final String path;

        ValuePathTuple(String value, String path) {
            this.value = value;
            this.path = path;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((path == null) ? 0 : path.hashCode());
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj){
                return true;
            }
            if (obj == null){
                return false;
            }
            if (getClass() != obj.getClass()){
                return false;
            }
            ValuePathTuple other = (ValuePathTuple) obj;
            if (!getOuterType().equals(other.getOuterType())){
                return false;
            }
            if (path == null) {
                if (other.path != null){
                    return false;
                }
            } else if (!path.equals(other.path)){
                return false;
            }
            if (value == null) {
                if (other.value != null){
                    return false;
                }
            } else if (!value.equals(other.value)){
                return false;
            }
            return true;
        }

        @Override
        public int compareTo(ValuePathTuple o) {
            if (this.equals(o)){
                return 0;
            }
            if (this.value.compareTo(o.value) < 0){
                return -1;
            }
            if (this.value.compareTo(o.value) > 0){
                return 1;
            }
            if (this.path.compareTo(o.path) < 0){
                return -1;
            }
            if (this.path.compareTo(o.path) > 0){
                return 1;
            }
            return 0;
        }

        private OrderedPropertyIndexQueryTest getOuterType() {
            return OrderedPropertyIndexQueryTest.this;
        }

    }

    /**
     * testing for asserting the right comparison behaviour of the custom class
     */
    @Test
    public void valuePathTupleComparison() {
        try {
            new ValuePathTuple("value", "path").compareTo(null);
            fail("It should have raised a NPE");
        } catch (NullPointerException e) {
            // so far so good
        }
        assertEquals(0, (new ValuePathTuple("value", "path")).compareTo(new ValuePathTuple("value", "path")));
        assertEquals(-1, (new ValuePathTuple("value", "path")).compareTo(new ValuePathTuple("value1", "path")));
        assertEquals(-1, (new ValuePathTuple("value1", "path")).compareTo(new ValuePathTuple("value1", "path1")));
        assertEquals(1, (new ValuePathTuple("value1", "path")).compareTo(new ValuePathTuple("value", "path")));
        assertEquals(1, (new ValuePathTuple("value1", "path1")).compareTo(new ValuePathTuple("value1", "path")));

        assertEquals(-1,
            (new ValuePathTuple("value000", "/test/n1")).compareTo(new ValuePathTuple("value001", "/test/n0")));
        assertEquals(1,
            (new ValuePathTuple("value001", "/test/n0")).compareTo(new ValuePathTuple("value000", "/test/n1")));
    }

    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new InitialContent())
            .with(new OpenSecurityProvider())
            // .with(new PropertyIndexProvider())
            // .with(new PropertyIndexEditorProvider())
            .with(new OrderedPropertyIndexProvider()).with(new OrderedPropertyIndexEditorProvider())
            .createContentRepository();
    }

    /**
     * create a child node for the provided father
     * 
     * @param father
     * @param name
     *            the name of the node to create
     * @param propName
     *            the name of the property to assign
     * @param propValue
     *            the value of the property to assign
     * @return
     */
    private static Tree child(Tree father, String name, String propName, String propValue) {
        Tree child = father.addChild(name);
        child.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
        child.setProperty(propName, propValue, Type.STRING);
        return child;
    }

    /**
     * generate a list of values to be used as ordered set. Will return something like
     * {@code value000, value001, value002, ...}
     * 
     * 
     * @param amount
     * @return
     */
    private static List<String> generateOrderedValues(int amount) {
        if (amount > 1000){
            throw new RuntimeException("amount cannot be greater than 100");
        }
        List<String> values = new ArrayList<String>(amount);
        NumberFormat nf = new DecimalFormat("000");
        for (int i = 0; i < amount; i++){
            values.add(String.format("value%s", String.valueOf(nf.format(i))));
        }
        return values;
    }

    /**
     * convenience method that adds a bunch of nodes in random order and return the order in which they should be
     * presented by the OrderedIndex
     * 
     * @param values
     *            the values of the property that will be indexed
     * @param father
     *            the father under which add the nodes
     * @return
     */
    private List<ValuePathTuple> addChildNodes(final List<String> values, final Tree father) {
        List<ValuePathTuple> nodes = new ArrayList<ValuePathTuple>();
        Random rnd = new Random();
        int counter = 0;
        while (!values.isEmpty()) {
            String v = values.remove(rnd.nextInt(values.size()));
            Tree t = child(father, String.format("n%s", counter++), ORDERED_PROPERTY, v);
            nodes.add(new ValuePathTuple(v, t.getPath()));
        }

        Collections.sort(nodes);
        return nodes;
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        IndexUtils.createIndexDefinition(new NodeUtil(index.getChild(IndexConstants.INDEX_DEFINITIONS_NAME)),
            TEST_INDEX_NAME, false, new String[] { ORDERED_PROPERTY }, null, OrderedIndex.TYPE);
        root.commit();
    }

    /**
     * assert the right order of the returned resultset
     * 
     * @param orderedSequence
     *            the right order in which the resultset should be returned
     * @param resultset
     *            the resultset
     */
    private void assertRightOrder(@Nonnull
    final List<ValuePathTuple> orderedSequence, @Nonnull
    final Iterator<? extends ResultRow> resultset) {
        assertTrue("No results returned", resultset.hasNext());
        int counter = 0;
        while (resultset.hasNext() && counter < orderedSequence.size()) {
            ResultRow row = resultset.next();
            assertEquals(String.format("Wrong path at the element '%d'", counter), orderedSequence.get(counter).path,
                row.getPath());
            counter++;
        }
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
        List<ValuePathTuple> nodes = addChildNodes(generateOrderedValues(NUMBER_OF_NODES), test);
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
        List<ValuePathTuple> nodes = addChildNodes(generateOrderedValues(NUMBER_OF_NODES), test);
        root.commit();

        ValuePathTuple searchfor = nodes.get(NUMBER_OF_NODES / 2); // getting the middle of the random list of
                                                                         // nodes.
        Map<String, PropertyValue> filter = ImmutableMap
            .of(ORDERED_PROPERTY, PropertyValues.newString(searchfor.value));
        String query = "SELECT * FROM [%s] WHERE %s=$%s";
        Iterator<? extends ResultRow> results = executeQuery(
            String.format(query, NT_UNSTRUCTURED, ORDERED_PROPERTY, ORDERED_PROPERTY), SQL2, filter).getRows()
            .iterator();
        assertTrue("one element is expected", results.hasNext());
        assertEquals("wrong path returned", searchfor.path, results.next().getPath());
        assertFalse("there should be not any more items", results.hasNext());

        setTravesalEnabled(true);
    }
}
