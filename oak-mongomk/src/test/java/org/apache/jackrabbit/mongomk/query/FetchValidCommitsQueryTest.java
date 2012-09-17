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
package org.apache.jackrabbit.mongomk.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.query.FetchValidCommitsQuery;
import org.apache.jackrabbit.mongomk.scenario.SimpleNodeScenario;
import org.junit.Test;


public class FetchValidCommitsQueryTest extends BaseMongoTest {

    private static final int MIN_COMMITS = 1;
    private static final int SIMPLE_SCENARIO_COMMITS = MIN_COMMITS + 1;

    @Test
    public void simple() throws Exception {
        FetchValidCommitsQuery query = new FetchValidCommitsQuery(mongoConnection,
                String.valueOf(Integer.MAX_VALUE) /*maxRevisionId*/);
        List<CommitMongo> commits = query.execute();
        assertEquals(MIN_COMMITS, commits.size());

        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        scenario.create();
        commits = query.execute();
        assertEquals(SIMPLE_SCENARIO_COMMITS, commits.size());

        int numberOfChildren = 3;
        scenario.addChildrenToA(numberOfChildren);
        commits = query.execute();
        assertEquals(SIMPLE_SCENARIO_COMMITS + numberOfChildren, commits.size());
    }

    @Test
    public void revisionId() throws Exception {
        FetchValidCommitsQuery query = new FetchValidCommitsQuery(mongoConnection,
                String.valueOf(Integer.MAX_VALUE) /*maxRevisionId*/);
        List<CommitMongo> commits = query.execute();
        CommitMongo commit0 = commits.get(0);

        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        scenario.create();
        commits = query.execute();
        CommitMongo commit1 = commits.get(0);
        assertTrue(commit0.getRevisionId() < commit1.getRevisionId());

        int numberOfChildren = 3;
        scenario.addChildrenToA(numberOfChildren);
        commits = query.execute();
        CommitMongo commit2 = commits.get(0);
        assertTrue(commit1.getRevisionId() < commit2.getRevisionId());
    }

    @Test
    public void time() throws Exception {
        FetchValidCommitsQuery query = new FetchValidCommitsQuery(mongoConnection,
                String.valueOf(Integer.MAX_VALUE) /*maxRevisionId*/);
        List<CommitMongo> commits = query.execute();
        CommitMongo commit0 = commits.get(0);

        Thread.sleep(1000);

        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        scenario.create();
        commits = query.execute();
        CommitMongo commit1 = commits.get(0);
        assertTrue(commit0.getTimestamp() < commit1.getTimestamp());

        Thread.sleep(1000);

        int numberOfChildren = 3;
        scenario.addChildrenToA(numberOfChildren);
        commits = query.execute();
        CommitMongo commit2 = commits.get(0);
        assertTrue(commit1.getTimestamp() < commit2.getTimestamp());
    }

    @Test
    public void maxEntriesDefaultLimitless() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        scenario.create();

        int numberOfChildren = 2;
        scenario.addChildrenToA(numberOfChildren);

        int maxEntries = 0;
        FetchValidCommitsQuery query = new FetchValidCommitsQuery(mongoConnection, maxEntries);
        List<CommitMongo> commits = query.execute();
        assertEquals(SIMPLE_SCENARIO_COMMITS + numberOfChildren, commits.size());
    }

    @Test
    public void maxEntries() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        scenario.create();

        int numberOfChildren = 2;
        scenario.addChildrenToA(numberOfChildren);

        int maxEntries = 2;
        FetchValidCommitsQuery query = new FetchValidCommitsQuery(mongoConnection, maxEntries);
        List<CommitMongo> commits = query.execute();
        assertEquals(maxEntries, commits.size());
    }
}
