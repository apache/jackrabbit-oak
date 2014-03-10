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

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
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
     * generate a list of values to be used as ordered set. Will return something like
     * {@code value000, value001, value002, ...}
     * 
     * 
     * @param amount
     * @param direction the direction of the sorting
     * @return a list of {@code amount} values ordered as specified by {@code direction}
     */
    protected static List<String> generateOrderedValues(int amount, OrderDirection direction) {
        if (amount > 1000) {
            throw new RuntimeException("amount cannot be greater than 1000");
        }
        List<String> values = new ArrayList<String>(amount);
        NumberFormat nf = new DecimalFormat("000");

        if (OrderDirection.DESC.equals(direction)) {
            for (int i = amount; i > 0; i--) {
                values.add(String.format("value%s", String.valueOf(nf.format(i))));
            }
        } else {
            for (int i = 0; i < amount; i++) {
                values.add(String.format("value%s", String.valueOf(nf.format(i))));
            }
        }
        return values;
    }
    
    /**
     * as {@code generateOrderedValues(int, OrderDirection)} by forcing OrderDirection.ASC
     * 
     * @param amount
     * @return
     */
    protected static List<String> generateOrderedValues(int amount) {
        return generateOrderedValues(amount, OrderDirection.ASC);
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
    static Tree child(Tree father, String name, String propName, String propValue) {
        Tree child = father.addChild(name);
        child.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
        child.setProperty(propName, propValue, Type.STRING);
        return child;
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
     * convenience method that adds a bunch of nodes in random order and return the order in which
     * they should be presented by the OrderedIndex
     * 
     * @param values the values of the property that will be indexed
     * @param father the father under which add the nodes
     * @param direction the direction of the items to be added.
     * @return
     */
    protected List<ValuePathTuple> addChildNodes(final List<String> values, final Tree father,
                                                 OrderDirection direction) {
        List<ValuePathTuple> nodes = new ArrayList<ValuePathTuple>();
        Random rnd = new Random();
        int counter = 0;
        while (!values.isEmpty()) {
            String v = values.remove(rnd.nextInt(values.size()));
            Tree t = child(father, String.format("n%s", counter++), ORDERED_PROPERTY, v);
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
     * @param orderedSequence the right order in which the resultset should be returned
     * @param resultset the resultset
     */
    protected void assertRightOrder(@Nonnull final List<ValuePathTuple> orderedSequence,
                                    @Nonnull final Iterator<? extends ResultRow> resultset) {
        assertTrue("No results returned", resultset.hasNext());
        int counter = 0;
        while (resultset.hasNext() && counter < orderedSequence.size()) {
            ResultRow row = resultset.next();
            assertEquals(
                String.format("Wrong path at the element '%d'", counter), 
                orderedSequence.get(counter).getPath(),
                row.getPath()
            );
            counter++;
        }
    }
}
