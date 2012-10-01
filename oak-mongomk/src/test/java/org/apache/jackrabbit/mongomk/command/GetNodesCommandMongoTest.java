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

import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.NodeAssert;
import org.apache.jackrabbit.mongomk.impl.builder.NodeBuilder;
import org.apache.jackrabbit.mongomk.scenario.SimpleNodeScenario;
import org.junit.Test;

/**
 * Tests GetNodesCommandMongo.
 */
public class GetNodesCommandMongoTest extends BaseMongoTest {

    @Test
    public void getNodesSimple() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        Long firstRevisionId = scenario.create();
        Long secondRevisionId = scenario.update_A_and_add_D_and_E();

        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection, "/",
                firstRevisionId, 0);
        Node actual = command.execute();
        Node expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : { \"a\" : {} } }",
                firstRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", secondRevisionId, 0);
        actual = command.execute();
        expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : { \"a\" : {} } }",
                firstRevisionId, secondRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);
    }

    @Test
    public void getNodesDepthLimited() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        Long firstRevisionId = scenario.create();
        Long secondRevisionId = scenario.update_A_and_add_D_and_E();

        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection, "/", firstRevisionId, 1);
        Node actual = command.execute();
        Node expected = NodeBuilder.build(
                String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b\" : {} , \"c\" : {} } } }",
                        firstRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", secondRevisionId, 1);
        actual = command.execute();
        expected = NodeBuilder.build(
                String.format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123, \"b\" : {} , \"c\" : {} , \"d\" : {} } } }",
                        firstRevisionId, secondRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", firstRevisionId, 2);
        actual = command.execute();
        expected = NodeBuilder.build(
                String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                        firstRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", secondRevisionId, 2);
        actual = command.execute();
        expected = NodeBuilder.build(
                String.format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" , \"e\" : {} } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                        firstRevisionId, secondRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", firstRevisionId, -1);
        actual = command.execute();
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                                firstRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", secondRevisionId, -1);
        actual = command.execute();
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                firstRevisionId, secondRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);
    }

    @Test
    public void getNodesDepthUnlimited() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        Long firstRevisionId = scenario.create();
        Long secondRevisionId = scenario.update_A_and_add_D_and_E();

        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection, "/", firstRevisionId, -1);
        Node actual = command.execute();
        Node expected = NodeBuilder.build(
                String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                        firstRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", secondRevisionId, -1);
        actual = command.execute();
        expected = NodeBuilder.build(
                String.format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                        firstRevisionId, secondRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);
    }

    @Test
    public void getNodesAfterDeletion() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        Long revisionId = scenario.create();
        revisionId = scenario.delete_A();

        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection, "/", revisionId, -1);
        Node actual = command.execute();
        Node expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }", revisionId));

        NodeAssert.assertDeepEquals(expected, actual);
    }

    @Test
    public void getNodesInvalidRevisionId() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        scenario.create();

        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection,
                "/", Long.MAX_VALUE, -1);
        try {
            command.execute();
            fail("Exception expected");
        } catch (Exception expected) {
        }
    }
}
