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
package org.apache.jackrabbit.oak.jcr;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Collections.reverseOrder;
import static java.util.Collections.sort;
import static javax.jcr.PropertyType.DATE;
import static javax.jcr.PropertyType.NAME;
import static javax.jcr.PropertyType.STRING;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.DIRECTION;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.TYPE;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection.DESC;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.oak.jcr.util.ValuePathTuple;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.test.RepositoryStub;
import org.apache.jackrabbit.test.RepositoryStubException;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class OrderedIndexIT {
    private static final String INDEX_DEF_NODE = "indexdef";
    private static final String ORDERED_PROPERTY = "foo";
    private static final String CONTENT = "content";
    private static final String NODE_TYPE = NT_UNSTRUCTURED;
    
    private static final Logger LOG = LoggerFactory.getLogger(OrderedIndexIT.class);

    private static final List<TimeZone> TZS = of(
        TimeZone.getTimeZone("GMT+01:00"),
        TimeZone.getTimeZone("GMT+02:00"), 
        TimeZone.getTimeZone("GMT+03:00"),
        TimeZone.getTimeZone("GMT+05:00"), 
        TimeZone.getTimeZone("GMT-02:00"),
        TimeZone.getTimeZone("GMT-04:00"), 
        TimeZone.getTimeZone("GMT-05:00"),
        TimeZone.getTimeZone("GMT-07:00"), 
        TimeZone.getTimeZone("GMT-08:00"),
        TimeZone.getTimeZone("GMT")
    );

    /** 
     * define the index
     * 
     * @param session
     * @throws RepositoryException
     */
    private void createIndexDefinition(@Nonnull final Session session) throws RepositoryException {
        checkNotNull(session);
        
        Node oakIndex = session.getRootNode().getNode(INDEX_DEFINITIONS_NAME);
        Node indexDef = oakIndex.addNode(INDEX_DEF_NODE, INDEX_DEFINITIONS_NODE_TYPE);
        indexDef.setProperty(TYPE_PROPERTY_NAME, TYPE, STRING);
        indexDef.setProperty(PROPERTY_NAMES, new String[]{ORDERED_PROPERTY} , NAME);
        indexDef.setProperty(DIRECTION, DESC.getDirection(), STRING);
        indexDef.setProperty(REINDEX_PROPERTY_NAME, true);
        session.save();
    }
    
    /**
     * add a bunch of sequential nodes with the provided property values pick-up randomly and start
     * numerating nodes from {@code startFrom}.
     * 
     * The caller will have to perform the {@link Session#save()} after the method call.
     * 
     * @param values the property values to set
     * @param father under whom we should add the nodes
     * @param propertyType the type of the property to be stored
     * @return
     * @throws RepositoryException 
     * @throws LockException 
     * @throws ConstraintViolationException 
     * @throws VersionException 
     * @throws PathNotFoundException 
     * @throws ItemExistsException 
     */
    private List<ValuePathTuple> addNodes(@Nonnull final List<String> values,
                                          @Nonnull final Node father,
                                          final int propertyType,
                                          final int startFrom) throws RepositoryException {
        
        checkNotNull(father);
        checkArgument(startFrom >= 0, "startFrom must be >= 0");
        
        List<String> working =  newArrayList(checkNotNull(values));
        Random rnd = new Random(1);
        int counter = startFrom;
        Node n;
        List<ValuePathTuple> vpt = newArrayList();
        
        while (!working.isEmpty()) {
            String v = working.remove(rnd.nextInt(working.size()));
            n = father.addNode("n" + counter++, NODE_TYPE);
            n.setProperty(ORDERED_PROPERTY, v, propertyType);
            vpt.add(new ValuePathTuple(v, n.getPath()));            
        }
        
        return vpt;
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void oak2035() throws IOException, RepositoryException, RepositoryStubException  {
        final int numberOfNodes = 1500;
        final String statement = String.format(
            "/jcr:root/content//element(*, %s) order by @%s descending", NODE_TYPE,
            ORDERED_PROPERTY);
        
        Properties env = new Properties();
        env.load(getClass().getResourceAsStream("/repositoryStubImpl.properties"));
        RepositoryStub stub = RepositoryStub.getInstance(env);
        Repository repo = stub.getRepository();
        Session session = null;
        Node root, content;
        
        try {
            session = repo.login(stub.getSuperuserCredentials());

            createIndexDefinition(session);
            
            root = session.getRootNode();
            if (!root.hasNode(CONTENT)) {
                root.addNode(CONTENT, NT_OAK_UNSTRUCTURED);
            }
            session.save();
            content = root.getNode(CONTENT);

            Calendar start = midnightFirstJan2013();
            List<String> dates = generateOrderedDates(String.class, numberOfNodes, DESC, start,
                DAY_OF_MONTH, 1, TZS, true);
            
            List<ValuePathTuple> nodes = addNodes(dates, content, DATE, 0);
            session.save();
            
            // ensuring the correct order for checks
            sort(nodes, reverseOrder());
            
            if (LOG.isDebugEnabled()) {
                for (ValuePathTuple node : nodes) {
                    LOG.debug(node.toString());
                }
            }
            
            QueryManager qm = session.getWorkspace().getQueryManager();
            Query query = qm.createQuery(statement, Query.XPATH);
            QueryResult result = query.execute();

            assertRightOrder(nodes, result.getRows());
            
        } finally {
            if (session != null) {
                session.logout();
            }
        }                
    }
 
    private void assertRightOrder(final List<ValuePathTuple> expected, 
                                  final RowIterator obtained) throws RepositoryException {
        checkNotNull(expected);
        checkNotNull(obtained);
        
        assertTrue("the obtained result is empty", obtained.hasNext());
        
        Iterator<ValuePathTuple> exp = expected.iterator();
        
        while (exp.hasNext() && obtained.hasNext()) {
            ValuePathTuple vpt = exp.next();
            Row row = obtained.nextRow();
            
            // check manually about paths and dates
            // if paths don't match maybe the date does. It's the date we care, in case of multiple
            // paths under the same date the order of them is non-deterministic dependent on
            // persistence rules
            if (!vpt.getPath().equals(row.getPath())) {
                String property = row.getNode().getProperty(ORDERED_PROPERTY).getString();
                if (!vpt.getValue().equals(property)) {
                    fail(String.format(
                        "both path and date failed to match. Expected: %s - %s. Obtained: %s, %s",
                        vpt.getPath(),
                        vpt.getValue(),
                        row.getPath(),
                        property
                        ));
                }
            }
        }
        
        assertFalse("we should have processed all the expected", exp.hasNext());
        assertFalse("we should have processed all the obtained", obtained.hasNext());
    }

    // ------------------------------------- < copied over from BasicOrderedPropertyIndexQueryTest>
    // TODO should we have anything in commons?
    
    /**
     * generates a list of sorted dates as ISO8601 formatted strings.
     * 
     * @param returnType Allowed values: {@link String} and {@link Long}. When String is specified
     *            it will return ISO8601 formatted dates, otherwise the milliseconds.
     * @param amount the amount of dates to be generated
     * @param direction the direction of the sorting for the dates
     * @param start the dates from where to start generating.
     * @param increaseOf the {@link Calendar} field to increase while generating
     * @param increaseBy the amount of increase to be used.
     * @param timezones available timezones to be used in a random manner
     * @param generateDuplicates if true it will generate a duplicate with a probability of 10%
     * @return
     */
    public static List<String> generateOrderedDates(Class<?> returnType, 
                                                    int amount,
                                                   @Nonnull OrderDirection direction,
                                                   @Nonnull final Calendar start,
                                                   int increaseOf,
                                                   int increaseBy,
                                                   @Nonnull List<TimeZone> timezones,
                                                   boolean generateDuplicates) {
        final Set<Integer> allowedIncrease = ImmutableSet.of(HOUR_OF_DAY, DAY_OF_MONTH);
        
        checkArgument(amount > 0, "the amount must be > 0");
        checkNotNull(direction);
        checkNotNull(start);
        checkArgument(allowedIncrease.contains(increaseOf), "Wrong increaseOf. Allowed values: "
                                                            + allowedIncrease);
        checkArgument(increaseBy > 0, "increaseBy must be a positive number");
        checkNotNull(timezones);
        checkArgument(returnType.equals(String.class) || returnType.equals(Long.class),
            "only String and Long accepted as return type");

        final int tzsSize = timezones.size();
        final boolean tzsExtract = tzsSize > 0;
        final Random rnd = new Random(1);
        final Random duplicate = new Random(2);
        
        List<String> values = new ArrayList<String>(amount);
        Calendar lstart = (Calendar) start.clone();
        int hours = (OrderDirection.DESC.equals(direction)) ? -increaseBy : increaseBy;
        
        for (int i = 0; i < amount; i++) {
            if (tzsExtract) {
                lstart.setTimeZone(timezones.get(rnd.nextInt(tzsSize)));
            }
            if (returnType.equals(String.class)) {
                values.add(ISO8601.format(lstart));                
            } else if (returnType.equals(Long.class)) {
                values.add(String.valueOf(lstart.getTimeInMillis()));
            }
            if (generateDuplicates && duplicate.nextDouble() < 0.1) {
                // let's not increase the date
            } else {
                lstart.add(increaseOf, hours);
            }
            
        }
        
        return values;        
    }
    
    /**
     * @return a Calendar set for midnight of 1st January 2013
     */
    public static Calendar midnightFirstJan2013() {
        Calendar c = Calendar.getInstance();
        c.set(2013, Calendar.JANUARY, 1, 0, 0, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c;
    }

}
