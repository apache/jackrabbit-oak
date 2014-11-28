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
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public abstract class BasicOrderedPropertyIndexQueryTest extends AbstractQueryTest {
    /**
     * the property used by the index
     */
    public static final String ORDERED_PROPERTY = "foo";

    /**
     * number of nodes to create for testing.
     *
     * It has been found during development that in some cases the order of the nodes creation
     * within the persistence where the actual expected order.
     *
     * The higher the value the lower the chance for this to happen.
     */
    protected static final int NUMBER_OF_NODES = 50;

    /**
     * formatter for date conversions
     */
    protected static final String ISO_8601_2000 = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"; 

    /**
     * same as {@link #generateOrderedValues(int, int, OrderDirection)} by passing {@code 0} as
     * {@code offset}
     * 
     * @param amount
     * @param direction
     * @return
     */
    protected static List<String> generateOrderedValues(int amount, OrderDirection direction) {
        return generateOrderedValues(amount, 0, direction);
    }

    /**
     * <p>
     * generate a list of values to be used as ordered set. Will return something like
     * {@code value000, value001, value002, ...}
     * </p>
     *
     * @param amount the values to be generated
     * @param offset move the current counter by this provided amount.
     * @param direction the direction of the sorting
     * @return a list of {@code amount} values ordered as specified by {@code direction}
     */
    protected static List<String> generateOrderedValues(int amount, int offset , OrderDirection direction) {
        if (amount > 1000) {
            throw new RuntimeException("amount cannot be greater than 1000");
        }
        List<String> values = new ArrayList<String>(amount);
        

        if (OrderDirection.DESC.equals(direction)) {
            for (int i = amount; i > 0; i--) {
                values.add(formatNumber(i + offset));
            }
        } else {
            for (int i = 0; i < amount; i++) {
                values.add(formatNumber(i + offset));
            }
        }
        return values;
    }
    
    /**
     * formats the provided number for being used by the
     * {@link #generateOrderedValues(int, OrderDirection)}
     * 
     * @param number
     * @return something in the format {@code value000}
     */
    public static String formatNumber(int number) {
        NumberFormat nf = new DecimalFormat("0000");
        return String.format("value%s", String.valueOf(nf.format(number)));
    }

    /**
     * as {@link #generateOrderedValues(int, OrderDirection)} by forcing {@link OrderDirection.ASC}
     *
     * @param amount
     * @return
     */
    protected static List<String> generateOrderedValues(int amount) {
        return generateOrderedValues(amount, OrderDirection.ASC);
    }

    /**
     * As {@link #child(Tree, String, String, String, Type)} but forces {@code String} as
     * {@code Type}.
     */
    static Tree child(Tree father, String name, String propName, String propValue) {
        return child(father, name, propName, propValue, Type.STRING);
    }

    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new LowCostOrderedPropertyIndexProvider())
            .with(new OrderedPropertyIndexEditorProvider())
            .createContentRepository();
    }

    /**
     * <p>
     * convenience method that adds a bunch of nodes in random order and return the order in which
     * they should be presented by the OrderedIndex.
     * </p>
     * <p>
     * The nodes will be created using the {@link #ORDERED_PROPERTY} as property for indexing
     * </p>
     * @param values the values of the property that will be indexed
     * @param father the father under which add the nodes
     * @param direction the direction of the items to be added.
     * @return
     */
    @SuppressWarnings("rawtypes")
    protected List<ValuePathTuple> addChildNodes(final List<String> values, final Tree father,
                                                 OrderDirection direction,
                                                 @Nonnull final Type propertyType) {
        List<ValuePathTuple> nodes = new ArrayList<ValuePathTuple>();
        Random rnd = new Random();
        int counter = 0;
        while (!values.isEmpty()) {
            String v = values.remove(rnd.nextInt(values.size()));
            Tree t = child(father, String.format("n%s", counter++), ORDERED_PROPERTY, v,
                propertyType);
            nodes.add(new ValuePathTuple(v, t.getPath()));
        }

        if (OrderDirection.DESC.equals(direction)) {
            Collections.sort(nodes, Collections.reverseOrder());
        } else {
            Collections.sort(nodes);
        }
        return nodes;
    }

    /**
     * assert the right order of the returned resultset
     *
     * @param expected the right order in which the resultset should be returned
     * @param resultset the resultset
     */
    protected void assertRightOrder(@Nonnull final List<ValuePathTuple> expected,
                                    @Nonnull final Iterator<? extends ResultRow> resultset) {
        if (expected.isEmpty()) {
            assertFalse("An empty resultset is expected but something has been returned.",
                resultset.hasNext());
        } else {
            assertTrue("No results returned", resultset.hasNext());
            int counter = 0;
            while (resultset.hasNext() && counter < expected.size()) {
                ResultRow row = resultset.next();
                assertEquals(
                    String.format("Wrong path at the element '%d'", counter),
                    expected.get(counter).getPath(),
                    row.getPath()
                );
                counter++;
            }
        }        
    }

    /**
     * convenience method for generating a list of ordered dates as string in ISO
     * 8601:2000-compliant format.
     *
     * {@link http ://www.day.com/specs/jcr/2.0/3_Repository_Model.html#3.6.4.3%20From%20DATE%20To}.
     *
     * it will add or remove depending of the {@code direction} provided, 12hrs for every entry to
     * be generated.
     *
     *
     * @param amount
     * @param direction the direction of the sorting. If null the {@code OrderDirection.ASC} will be
     *            used
     * @param start the date from which to start from in the generation
     * @return a list of {@code amount} values ordered as specified by {@code direction}
     */
    protected static List<String> generateOrderedDates(int amount,
                                                       OrderDirection direction,
                                                       @Nonnull final Calendar start) {
        if (amount > 1000) {
            throw new RuntimeException("amount cannot be greater than 1000");
        }
        List<String> values = new ArrayList<String>(amount);
        Calendar lstart = (Calendar) start.clone();
        int hours = (OrderDirection.DESC.equals(direction)) ? -12 : 12;
        SimpleDateFormat sdf = new SimpleDateFormat(ISO_8601_2000);
        for (int i = 0; i < amount; i++) {
            values.add(sdf.format(lstart.getTime()));
            lstart.add(Calendar.HOUR_OF_DAY, hours);
        }

        return values;
    }

    /**
     * create a child node for the provided father
     *
     * @param father
     * @param name the name of the node to create
     * @param propName the name of the property to assign
     * @param propValue the value of the property to assign
     * @param type the type of the property
     * @return the just added child
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static Tree child(@Nonnull Tree father, @Nonnull String name, @Nonnull String propName,
                      @Nonnull String propValue, @Nonnull Type type) {
        Tree child = father.addChild(name);
        child.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
        child.setProperty(propName, propValue, type);
        return child;
    }

    /**
     * @return a Calendar set for midnight of 1st January 2013
     */
    protected static Calendar midnightFirstJan2013() {
        Calendar c = Calendar.getInstance();
        c.set(2013, Calendar.JANUARY, 1, 0, 0);
        return c;
    }

    /**
     * convenience method that adds a bunch of nodes in random order and return the order in which
     * they should be presented by the OrderedIndex
     *
     * @param values the values of the property that will be indexed
     * @param father the father under which add the nodes
     * @return
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected void addChildNodes(final List<String> values,
                                 final NodeBuilder father,
                                 @Nonnull final Type propertyType) {
        Random rnd = new Random();
        int counter = 0;
        while (!values.isEmpty()) {
            String v = values.remove(rnd.nextInt(values.size()));
            father.child(String.format("n%s", counter++))
                .setProperty(ORDERED_PROPERTY, v, propertyType);
        }
    }

    /**
     * initiate the environment for testing with proper OrderedPropertyIndexProvider
     * @throws Exception
     */
    protected void initWithProperProvider() throws Exception {
        session = new Oak().with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new OrderedPropertyIndexProvider())
            .with(new OrderedPropertyIndexEditorProvider())
            .createContentRepository().login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
        createTestIndexNode();
    }
}
