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
package org.apache.jackrabbit.oak.plugins.index.old.mk.wrapper;

import static org.apache.jackrabbit.oak.plugins.index.old.Indexer.INDEX_CONFIG_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.plugins.index.old.mk.IndexWrapper;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.SimpleKernelImpl;
import org.junit.Test;

/**
 * Test the index wrapper.
 */
public class IndexWrapperTest {

    // TODO: Remove SimpleKernelImpl-specific assumptions from the test
    private final MicroKernel mk =
            new IndexWrapper(new SimpleKernelImpl("mem:IndexWrapperTest"));

    private String head;

    @Test
    public void getNodes() {
        assertNull(mk.getNodes(INDEX_CONFIG_PATH + "/unknown", head, 1, 0, -1, null));
        assertNull(mk.getNodes("/unknown", head, 1, 0, -1, null));
    }

    @Test
    public void prefix() {
        head = mk.commit(INDEX_CONFIG_PATH, "+ \"prefix@x\": {}", head, "");
        head = mk.commit("/", "+ \"n1\": { \"value\":\"a:no\" }", head, "");
        head = mk.commit("/", "+ \"n2\": { \"value\":\"x:yes\" }", head, "");
        head = mk.commit("/", "+ \"n3\": { \"value\":\"x:a\" }", head, "");
        head = mk.commit("/", "+ \"n4\": { \"value\":\"x:a\" }", head, "");
        String empty = mk.getNodes(INDEX_CONFIG_PATH + "/prefix@x?x:no", head, 1, 0, -1, null);
        assertEquals("[]", empty);
        String yes = mk.getNodes(INDEX_CONFIG_PATH + "/prefix@x?x:yes", head, 1, 0, -1, null);
        assertEquals("[\"/n2/value\"]", yes);
        String a = mk.getNodes(INDEX_CONFIG_PATH + "/prefix@x?x:a", head, 1, 0, -1, null);
        assertEquals("[\"/n3/value\",\"/n4/value\"]", a);
    }

    @Test
    public void propertyUnique() {
        head = mk.commit(INDEX_CONFIG_PATH, "+ \"property@id,unique\": {}", head, "");
        head = mk.commit("/", "+ \"n1\": { \"value\":\"empty\" }", head, "");
        head = mk.commit("/", "+ \"n2\": { \"id\":\"1\" }", head, "");
        String empty = mk.getNodes(INDEX_CONFIG_PATH + "/property@id,unique?0", head, 1, 0, -1, null);
        assertEquals("[]", empty);
        String one = mk.getNodes(INDEX_CONFIG_PATH + "/property@id,unique?1", head, 1, 0, -1, null);
        assertEquals("[\"/n2\"]", one);
    }

    @Test
    public void propertyNonUnique() {
        head = mk.commit(INDEX_CONFIG_PATH, "+ \"property@ref\": {}", head, "");
        head = mk.commit("/", "+ \"n1\": { \"ref\":\"a\" }", head, "");
        head = mk.commit("/", "+ \"n2\": { \"ref\":\"b\" }", head, "");
        head = mk.commit("/", "+ \"n3\": { \"ref\":\"b\" }", head, "");
        String empty = mk.getNodes(INDEX_CONFIG_PATH + "/property@ref?no", head, 1, 0, -1, null);
        assertEquals("[]", empty);
        String one = mk.getNodes(INDEX_CONFIG_PATH + "/property@ref?a", head, 1, 0, -1, null);
        assertEquals("[\"/n1\"]", one);
        String two = mk.getNodes(INDEX_CONFIG_PATH + "/property@ref?b", head, 1, 0, -1, null);
        assertEquals("[\"/n2\",\"/n3\"]", two);
    }

}
