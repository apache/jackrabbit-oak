/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.old.mk.large;

import org.apache.jackrabbit.oak.plugins.index.old.mk.MultiMkTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Creates and then reads nodes distributed in a tree with random width at each
 * level.
 */
@RunWith(Parameterized.class)
public class CreateRandomNodesTraverseTest extends MultiMkTestBase {

    public CreateRandomNodesTraverseTest(String url) {
        super(url);
    }

    @Test
    public void test() throws Exception {
        RandomNodeCreator c = new RandomNodeCreator(mk, 1);
        c.setTotalCount(200);
        c.setMaxWidth(10);
        // c.setLogToSystemOut(true);

        // 1 million node test
        // c.setLogToSystemOut(true);
        // c.setTotalCount(1000000);

        // 20 million node test
        // c.setTotalCount(20000000);

        c.create();
        c.traverse();
    }
}