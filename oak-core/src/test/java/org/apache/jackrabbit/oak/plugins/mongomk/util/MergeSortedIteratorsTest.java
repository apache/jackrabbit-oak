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
package org.apache.jackrabbit.oak.plugins.mongomk.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Ordering;
import org.apache.jackrabbit.oak.plugins.mongomk.Revision;
import org.apache.jackrabbit.oak.plugins.mongomk.StableRevisionComparator;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import static com.mongodb.util.MyAsserts.assertTrue;
import static junit.framework.Assert.assertEquals;

/**
 * Tests for {@link MergeSortedIterators}.
 */
public class MergeSortedIteratorsTest {

    @SuppressWarnings("unchecked")
    @Test
    public void test() {
        assertEquals(list(), sort());
        assertEquals(list(), sort(list()));
        assertEquals(list(1, 2, 3, 4), sort(list(1, 2), list(3, 4)));
        assertEquals(list(1, 2, 3, 4), sort(list(1, 3), list(2, 4)));
        assertEquals(list(1, 2, 3), sort(list(1, 3), list(2)));
        assertEquals(list(1, 2, 3, 4), sort(list(1, 4), list(2, 3)));
        assertEquals(list(1, 2, 3, 4, 5, 6), sort(list(1, 5), list(2, 4), list(3, 6)));
    }

    @Ignore("OAK-1233")
    @Test
    public void testData(){
        List<Iterator<Revision>> iterators = prepareData();
        final Iterator<Iterator<Revision>> it = iterators.iterator();
        final Comparator<Revision> comp = Collections.reverseOrder(new StableRevisionComparator());
        MergeSortedIterators<Revision> sort = new MergeSortedIterators<Revision>(comp) {
            @Override
            public Iterator<Revision> nextIterator() {
                return it.hasNext() ? it.next() : null;
            }
        };
        List<Revision> sorted = new ArrayList<Revision>();
        while (sort.hasNext()) {
            sorted.add(sort.next());
        }

        assertTrue(Ordering.from(comp).isOrdered(sorted));
    }

    private List<Integer> sort(List<Integer>... lists) {
        List<Iterator<Integer>> iterators = Lists.newArrayList();
        for (List<Integer> list : lists) {
            iterators.add(list.iterator());
        }
        final Iterator<Iterator<Integer>> it = iterators.iterator();
        MergeSortedIterators<Integer> sort = new MergeSortedIterators<Integer>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        }) {
            @Override
            public Iterator<Integer> nextIterator() {
                return it.hasNext() ? it.next() : null;
            }
        };
        List<Integer> sorted = new ArrayList<Integer>();
        while (sort.hasNext()) {
            sorted.add(sort.next());
        }
        return sorted;
    }

    final static String[][] TEST_DATA = {
            {"r142999329fe-0-2","r142999329b8-0-2","r14299931664-0-2","r14299931626-0-2","r142999302e2-0-2",
                    "r1429993029e-0-2","r1429992ef5c-0-2","r1429992ef17-0-2","r1429992dbd3-0-2","r1429992db8e-0-2",
                    "r1429992c84e-0-2","r1429992c807-0-2","r1429992b4c0-0-2","r1429992b47e-0-2","r1429992a13b-0-2",
                    "r1429992a0f6-0-2","r14299928db5-0-2","r14299928d6f-0-2","r14299927a31-0-2","r142999279e7-0-2",
                    "r142999266a6-0-2","r1429992665f-0-2","r1429992531e-0-2","r142999252d7-0-2","r14299923f9a-0-2",
                    "r14299923f4f-0-2","r14299922c0e-0-2","r14299922bc7-0-2","r14299921891-0-2","r1429992183e-0-2",
                    "r142999204fe-0-2","r142999204b6-0-2","r1429991f18b-0-2","r1429991f12f-0-2","r1429991de04-0-2",
                    "r1429991dda7-0-2","r1429991ca6b-0-2","r1429991ca1f-0-2","r1429991b6dc-0-2","r1429991b697-0-2",
                    "r1429991a354-0-2","r14299797f8e-0-1","r142995ec597-0-1","r142995d604e-0-1","r1429956ef34-0-1",
                    "r142995557b5-0-1","r1429954ed51-0-1","r142992d1e45-0-1","r14298f1183d-2-1","r14298f0fe7c-0-1",
                    "r14298f0f1e9-0-1","r14298f0b75d-0-1","r14298f0b110-0-1","r14298f08964-0-1","r14298f04ef8-0-1",
                    "r14298f01427-0-1","r14298f0079a-0-1","r14298ef9f12-0-1","r14298ef64a4-0-1","r14298ef1d4c-0-1",
                    "r14298ef16f6-0-1","r14298eea854-2-1","r14298eea1c3-0-1","r14298ee6458-0-1","r14298ee5ada-0-1",
                    "r14298ee1de1-0-1","r14298ee04d6-0-1","r14298edcbd9-0-1","r14298edb300-0-1","r14298eda8da-0-1",
                    "r14298ed8fc6-0-1","r14298ed563a-0-1","r14298ecf9a3-0-1","r14298ecbe57-0-1","r14298eca547-0-1",
                    "r14298eba8f0-0-1","r14298eb997a-0-1","r14298eb806a-0-1","r14298eb2ffa-0-1","r14298eb2678-0-1",
                    "r14298eae9b7-0-1","r14298ead0ad-0-1","r14298ea95ef-0-1","r14298ea4eb5-0-1","r14298ea4867-0-1",
                    "r14298ea1421-0-1","r14298ea0dd5-0-1","r14298e9e612-0-1","r14298e98c33-0-1","r14298e97679-0-1",
                    "r14298e95d6f-0-1","r14298e8e82f-0-1","r14298e8adaf-0-1","r14298e85c03-0-1","r14298e85186-0-1",
                    "r14298e83879-0-1","r14298e7eaee-0-1","r14298e7dc55-0-1","r14298e7c349-0-1","r14298e74f04-0-1",
                    "r14298e73697-0-1","r14298e72c7a-0-1","r14298e71372-0-1","r14298e664f5-0-1","r14298e64c82-0-1",
                    "r14298e64237-0-1","r14298e62928-0-1","r14298e5e1f3-0-1","r14298e5dba9-0-1","r14298e5b497-0-1",
                    "r14298e4764d-0-1","r14298e46b33-0-2","r14298dc54b3-0-1","r14298c0b683-0-2",
            },
            {"r1429991a30f-0-2","r14299918fca-0-2","r14299918f86-0-2","r14299917c45-0-2","r14299917bfe-0-2",
                    "r142999168c8-2-2","r14299916876-0-2","r14299915536-0-2","r142999154ee-0-2","r142999141b1-0-2",
                    "r14299914166-0-2","r14299912e27-0-2","r14299912dde-0-2","r14299911a9c-0-2","r14299911a56-0-2",
                    "r14299910713-0-2","r142999106ce-0-2","r1429990f38f-0-2","r1429990f346-0-2","r1429990e004-0-2",
                    "r1429990dfbe-0-2","r1429990cc79-0-2","r1429990cc36-0-2","r1429990b8f5-0-2","r1429990b8ae-0-2",
                    "r1429990a578-0-2","r1429990a527-0-2","r142999091e9-0-2","r1429990919f-0-2","r14299907e61-0-2",
                    "r14299907e16-0-2","r14299906ad7-0-2","r14299906a8f-0-2","r14299905759-0-2","r14299905706-0-2",
                    "r142999043da-0-2","r1429990437f-0-2","r1429990303c-0-2","r14299902ff6-0-2","r14299901cb3-0-2",
                    "r14299901c6e-0-2","r1429990093b-0-2","r142999008e6-0-2","r142998ff5b4-0-2","r142998ff55e-0-2",
                    "r142998fe21e-0-2","r142998fe1d7-0-2","r142998fce98-0-2","r142998fce4f-0-2","r142998fbb11-0-2",
                    "r142998fbac7-0-2","r142998fa793-0-2","r142998fa73f-0-2","r142998f93fc-0-2","r142998f93b7-0-2",
                    "r142998f807e-0-2","r142998f802e-0-2","r142998f6ce1-0-2","r142998f6ca7-0-2","r142998f596c-0-2",
                    "r142998f591f-0-2","r142998f45e4-0-2","r142998f4597-0-2","r142998f3259-0-2","r142998f320f-0-2",
                    "r142998f1ed9-0-2","r142998f1e87-0-2","r142998f0b3e-0-2","r142998f0afe-0-2","r142998ef7c9-0-2",
                    "r142998ef777-0-2","r142998ee42a-0-2","r142998ee3ef-0-2","r142998ed0b1-0-2","r142998ed067-0-2",
                    "r142998ebd28-0-2","r142998ebcdf-0-2","r142998ea994-0-2","r142998ea956-0-2","r142998e9614-0-2",
                    "r142998e95ce-0-2","r142998e8289-0-2","r142998e8247-0-2","r142998e6efd-0-2","r142998e6ebe-0-2",
                    "r142998e5b7d-0-2","r142998e5b36-0-2","r142998e47fb-0-2","r142998e47ae-0-2","r142998e346e-0-2",
                    "r142998e3427-0-2","r142998e20da-0-2","r142998e209e-0-2","r142998e0d63-0-2","r142998e0d16-0-2",
                    "r142998dfa1f-0-2","r142998df98e-0-2","r142998de67c-0-2","r142998de606-0-2","r142998dd2c8-0-2",
                    "r142998dd27f-0-2","r142998dbf42-0-2","r142998dbef6-0-2","r142998dabbb-0-2","r142998dab6f-0-2",
                    "r142998d9827-0-2","r142998d97e6-0-2","r142998d84b3-0-2","r142998d845e-0-2","r142998d713b-0-2",
                    "r142998d70d6-0-2","r142998d5d9a-0-2","r142998d5d4e-0-2","r142998d4a0c-0-2","r142998d49c6-0-2",
                    "r142998d3678-0-2","r142998d363e-0-2","r142998d22fe-0-2","r142998d22b6-0-2","r142998d0fbb-0-2",
                    "r142998d0f2f-0-2","r142998cfc01-0-2","r142998cfba7-0-2","r142998ce861-0-2","r142998ce81f-0-2",
                    "r142998cd4e2-0-2","r142998cd497-0-2","r142998cc14f-0-2",
            },
            {"r142998cc10e-0-2","r142998cadd1-0-2","r142998cad88-0-2","r142998c9a49-0-2","r142998c99fe-0-2",
                    "r142998c86b7-0-2","r142998c8676-0-2","r142998c7341-0-2","r142998c72ee-0-2","r142998c5fb0-0-2",
                    "r142998c5f67-0-2","r142998c4c1f-0-2","r142998c4bde-0-2","r142998c38a5-0-2","r142998c3857-0-2",
                    "r142998c2524-0-2","r142998c24cf-0-2","r142998c11d1-0-2","r142998c1146-0-2","r142998bfe00-0-2",
                    "r142998bfdbe-0-2","r142998bea79-0-2","r142998bea37-0-2","r142998bd708-0-2","r142998bd6af-0-2",
                    "r142998bc37b-0-2","r142998bc327-0-2","r142998bafde-0-2","r142998baf9e-0-2","r142998b9c74-0-2",
                    "r142998b9c16-0-2","r142998b88dc-0-2","r142998b888f-0-2","r142998b754c-0-2","r142998b7506-0-2",
                    "r142998b61d3-0-2","r142998b617e-0-2","r142998b4e3b-0-2","r142998b4df6-0-2","r142998b3aad-0-2",
                    "r142998b3a6e-0-2","r142998b2751-0-2","r142998b26e6-0-2","r142998b139f-0-2","r142998b135e-0-2",
                    "r142998b0025-0-2","r142998affd7-0-2","r142998aec98-0-2","r142998aec4e-0-2","r142998ad920-0-2",
                    "r142998ad8c6-0-2","r142998ac57e-0-2","r142998ac53e-0-2","r142998ab209-0-2","r142998ab1b7-0-2",
                    "r142998a9e75-0-2","r142998a9e2e-0-2","r142998a8afc-0-2","r142998a8aa7-0-2","r142998a7763-0-2",
                    "r142998a771e-0-2","r142998a63ef-0-2","r142998a6397-0-2","r142998a5054-0-2","r142998a500e-0-2",
                    "r142998a3cd1-0-2","r142998a3c87-0-2","r142998a2951-0-2","r142998a28ff-0-2","r142998a15c1-0-2",
                    "r142998a1576-0-2","r142998a0266-0-2","r142998a01ef-0-2","r1429989eeb2-0-2","r1429989ee66-0-2",
                    "r1429989db2d-0-2","r1429989dade-0-2","r1429989c7aa-0-2","r1429989c756-0-2","r1429989b41c-0-2",
                    "r1429989b3ce-0-2","r1429989a092-0-2","r1429989a046-0-2","r14299898d0d-0-2","r14299898cbe-0-2",
                    "r1429989797e-0-2","r14299897936-0-2","r1429989661d-0-2","r142998965ae-0-2","r1429989527d-0-2",
                    "r14299895226-0-2","r14299893f0d-0-2","r14299893e9f-0-2","r14299892b5f-0-2","r14299892b16-0-2",
                    "r14299891802-0-2","r1429989178f-0-2","r14299891563-0-2",
            },
            {},
            {},
            {"r14298e46af4-0-2","r14298e45beb-0-2"},
            {"r14298e4442a-0-2","r14298e443e5-0-2","r14298e43aac-0-2","r14298e43a04-0-2","r14298e41db8-0-2",
                    "r14298e40b53-0-2","r14298e4094f-0-2","r14298e3f701-0-2","r14298e3e2fa-0-2","r14298e3e23c-0-2",
                    "r14298e3d83e-0-2","r14298e3d7be-0-2","r14298e3bc27-0-2","r14298e3a9b3-0-2","r14298e3a7a7-0-2",
                    "r14298e3964a-0-2","r14298e39420-0-2","r14298e382ea-0-2","r14298e3809c-0-2","r14298e35b02-0-2",
                    "r14298e35986-0-2","r14298e34640-0-2","r14298e345fc-0-2","r14298e33cb5-0-2","r14298e33c29-0-2",
                    "r14298e320e3-0-2","r14298e2f97e-0-2","r14298e2d241-0-2","r14298e2d0ce-0-2","r14298e2bee9-0-2",
                    "r14298e2aa03-0-2","r14298e2a9bc-0-2","r14298e29a47-0-2","r14298e282ef-0-2","r14298e282ac-0-2",
                    "r14298e2792b-0-2","r14298e278a9-0-2","r14298e25c8a-0-2","r14298e24a23-0-2",
            },
            {"r14298e4548d-0-1","r14298e4294d-0-1","r14298e41673-0-1","r14298e414a7-0-1","r14298e402ee-0-1",
                    "r14298e40120-0-1","r14298e3ed95-0-1","r14298e3c83f-0-1","r14298e3b53b-0-1","r14298e3b2fd-0-1",
                    "r14298e3a1fa-0-1","r14298e39f75-0-1","r14298e38e00-0-1","r14298e38bf0-0-1","r14298e37a5b-0-1",
                    "r14298e37866-0-1","r14298e36737-0-1","r14298e364dd-0-1","r14298e351ee-0-1","r14298e35152-0-1",
                    "r14298e32bdd-0-1","r14298e317db-0-1","r14298e316be-0-1","r14298e30afa-0-1","r14298e30a55-0-1",
                    "r14298e2efac-0-1","r14298e2de47-0-1","r14298e2dc28-0-1","r14298e2caf0-0-1","r14298e2c8a0-0-1",
                    "r14298e2b51a-0-1","r14298e2930a-0-1","r14298e268b4-0-1","r14298e2559f-0-1","r14298e2536d-0-1",
            },
    };

    private static List<Iterator<Revision>> prepareData() {
        List<Iterator<Revision>> result = new ArrayList<Iterator<Revision>>();
        for (String[] revsString : TEST_DATA) {
            List<Revision> revs = new ArrayList<Revision>();
            for (String r : revsString) {
                revs.add(Revision.fromString(r));
            }
            result.add(revs.iterator());
        }
        return result;
    }

    private static List<Integer> list(int... values) {
        List<Integer> list = new ArrayList<Integer>();
        for (int v : values) {
            list.add(v);
        }
        return list;
    }
}
