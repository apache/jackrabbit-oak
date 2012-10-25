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
package org.apache.jackrabbit.oak.plugins.index.old.mk;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test large node, property, and values (longer than 65 KB).
 */
@RunWith(Parameterized.class)
public class LargeObjectTest extends MultiMkTestBase {

    private static final String TEST_PATH = "/" + LargeObjectTest.class.getName();

    public LargeObjectTest(String url) {
        super(url);
    }

    @Test
    public void test() {

        mk.commit("/", "+ \"" + TEST_PATH.substring(1) + "\": {}", mk.getHeadRevision(), null);

        String head = mk.getHeadRevision();
        head = mk.commit(TEST_PATH, "+ \"test\": { \"data\": \"Hello World\" }", head, null);

        String large = new String(new char[100000]).replace((char) 0, 'x');
        head = mk.commit(TEST_PATH, "^ \"test/data\": \"" + large + "\"", head, null);

        head = mk.commit(TEST_PATH, "+ \"test2\": { \"" + large + "\": \"Hello World\" }", head, null);
        head = mk.commit(TEST_PATH, "^ \"test2/" + large + "\": \"" + large + "\"", head, null);

        head = mk.commit(TEST_PATH, "+ \"" + large + "\": { \"" + large + "\": \"Hello World\" }", head, null);
        head = mk.commit(TEST_PATH, "^ \"" + large + "/" + large + "\": \"" + large + "\"", head, null);

        mk.commit("/", "- \"" + TEST_PATH.substring(1) + "\"", mk.getHeadRevision(), null);

    }

}
