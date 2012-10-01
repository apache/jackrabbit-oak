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
package org.apache.jackrabbit.mongomk.command;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.command.GetHeadRevisionCommandMongo;
import org.apache.jackrabbit.mongomk.scenario.SimpleNodeScenario;
import org.junit.Test;


@SuppressWarnings("javadoc")
public class GetHeadRevisionCommandMongoTest extends BaseMongoTest {

    @Test
    public void testGeadHeadRevisionSimple() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        Long revisionId = scenario.create();

        GetHeadRevisionCommandMongo command = new GetHeadRevisionCommandMongo(mongoConnection);
        Long revisionId2 = command.execute();
        assertTrue(revisionId == revisionId2);

        scenario.delete_A();
        Long revisionId3 = command.execute();
        assertFalse(revisionId3 == revisionId2);
    }}
