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
package org.apache.jackrabbit.oak.benchmark;

import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.fixture.JackrabbitRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Benchmark {

    private static final Map<String, RepositoryFixture> FIXTURES =
            ImmutableMap.<String, RepositoryFixture>builder()
            .put("Jackrabbit", new JackrabbitRepositoryFixture())
            .put("Oak-Memory", OakRepositoryFixture.getMemory())
            .put("Oak-Default", OakRepositoryFixture.getDefault())
            .put("Oak-Mongo", OakRepositoryFixture.getMongo())
            .put("Oak-NewMongo", OakRepositoryFixture.getNewMongo())
            .put("Oak-Segment", OakRepositoryFixture.getSegment())
            .build();

    private static final Map<String, AbstractTest> TESTS =
            ImmutableMap.<String, AbstractTest>builder()
            .put("Login",           new LoginTest())
            .put("LoginLogout",     new LoginLogoutTest())
            .put("GetProperty",     new ReadPropertyTest())
            .put("SetProperty",     new SetPropertyTest())
            .put("SmallRead",       new SmallFileReadTest())
            .put("SmallWrite",      new SmallFileWriteTest())
            .put("ConcurrentRead",  new ConcurrentReadTest())
            .put("ConcurrentWrite", new ConcurrentReadWriteTest())
            .put("SimpleSearch",    new SimpleSearchTest())
            .put("SQL2",            new SQL2SearchTest())
            .put("Descendant",      new DescendantSearchTest())
            .put("SQL2Descendant",  new SQL2DescendantSearchTest())
            .put("CreateFlatNode",  new CreateManyChildNodesTest())
            .put("UpdateFlatNode", new UpdateManyChildNodesTest())
            .put("TransientSpace", new TransientManyChildNodesTest())
            .build();

    public static void main(String[] args) throws Exception {
        Map<String, RepositoryFixture> fixtures = Maps.newLinkedHashMap();
        List<AbstractTest> tests = Lists.newArrayList();
        for (String name : args) {
            if (FIXTURES.containsKey(name)) {
                fixtures.put(name, FIXTURES.get(name));
            } else if (TESTS.containsKey(name)) {
                tests.add(TESTS.get(name));
            } else {
                throw new RuntimeException("Unknown argument: " + name);
            }
        }
        if (fixtures.isEmpty()) {
            fixtures.putAll(FIXTURES);
        }
        if (tests.isEmpty()) {
            tests.addAll(TESTS.values());
        }

        for (AbstractTest test : tests) {
            test.run(fixtures);
        }
    }

}
