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
package org.apache.jackrabbit.mk.wrapper;

import static org.junit.Assert.assertEquals;
import org.apache.jackrabbit.mk.MultiMkTestBase;
import org.apache.jackrabbit.mk.index.IndexWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test the index wrapper.
 */
@RunWith(Parameterized.class)
public class IndexWrapperTest extends MultiMkTestBase {

    private String head;

    public IndexWrapperTest(String url) {
        super(url);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mk = new IndexWrapper(mk);
    }

    @Test
    public void prefix() {
        if (!isSimpleKernel(mk)) {
            return;
        }
        head = mk.commit("/index", "+ \"prefix:x\": {}", head, "");
        head = mk.commit("/", "+ \"n1\": { \"value\":\"a:no\" }", head, "");
        head = mk.commit("/", "+ \"n2\": { \"value\":\"x:yes\" }", head, "");
        head = mk.commit("/", "+ \"n3\": { \"value\":\"x:a\" }", head, "");
        head = mk.commit("/", "+ \"n4\": { \"value\":\"x:a\" }", head, "");
        String empty = mk.getNodes("/index/prefix:x?x:no", head);
        assertEquals("[]", empty);
        String yes = mk.getNodes("/index/prefix:x?x:yes", head);
        assertEquals("[\"/n2/value\"]", yes);
        String a = mk.getNodes("/index/prefix:x?x:a", head);
        assertEquals("[\"/n3/value\",\"/n4/value\"]", a);
    }

    @Test
    public void propertyUnique() {
        if (!isSimpleKernel(mk)) {
            return;
        }
        head = mk.commit("/index", "+ \"property:id,unique\": {}", head, "");
        head = mk.commit("/", "+ \"n1\": { \"value\":\"empty\" }", head, "");
        head = mk.commit("/", "+ \"n2\": { \"id\":\"1\" }", head, "");
        String empty = mk.getNodes("/index/property:id,unique?0", head);
        assertEquals("[]", empty);
        String one = mk.getNodes("/index/property:id,unique?1", head);
        assertEquals("[\"/n2\"]", one);
    }

    @Test
    public void propertyNonUnique() {
        if (!isSimpleKernel(mk)) {
            return;
        }
        head = mk.commit("/index", "+ \"property:ref\": {}", head, "");
        head = mk.commit("/", "+ \"n1\": { \"ref\":\"a\" }", head, "");
        head = mk.commit("/", "+ \"n2\": { \"ref\":\"b\" }", head, "");
        head = mk.commit("/", "+ \"n3\": { \"ref\":\"b\" }", head, "");
        String empty = mk.getNodes("/index/property:ref?no", head);
        assertEquals("[]", empty);
        String one = mk.getNodes("/index/property:ref?a", head);
        assertEquals("[\"/n1\"]", one);
        String two = mk.getNodes("/index/property:ref?b", head);
        assertEquals("[\"/n2\",\"/n3\"]", two);
    }

}
