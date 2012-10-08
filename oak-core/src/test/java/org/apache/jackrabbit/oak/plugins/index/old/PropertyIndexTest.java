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
package org.apache.jackrabbit.oak.plugins.index.old;

import junit.framework.Assert;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.junit.Test;

/**
 * Test the property index.
 */
public class PropertyIndexTest {

    @Test
    public void test() {
        MicroKernel mk = new MicroKernelImpl();
        Indexer indexer = new Indexer(mk);
        indexer.init();
        PropertyIndex index = indexer.createPropertyIndex("id", true);

        String head = mk.getHeadRevision();

        // meta data
        String meta = mk.getNodes(Indexer.INDEX_CONFIG_PATH, head, 1, 0, -1, null);
        Assert.assertEquals("{\":childNodeCount\":2,\":data\":{\":childNodeCount\":0}," +
                "\"property@id,unique\":{\":childNodeCount\":1,\":data\":{}}}", meta);

        String oldHead = head;

        Assert.assertEquals(null, index.getPath("1", head));

        head = mk.commit("/", "+\"test\" : {\"id\":\"1\"}", head, null);
        head = mk.commit("/", "+\"test2\" : {\"id\":\"2\"}", head, null);

        Assert.assertEquals("/test", index.getPath("1", head));
        Assert.assertEquals("/test2", index.getPath("2", head));

        Assert.assertEquals("/test", index.getPath("1", oldHead));
        Assert.assertEquals("/test2", index.getPath("2", oldHead));

        Assert.assertEquals("/test", index.getPath("1", head));
        Assert.assertEquals("/test2", index.getPath("2", head));

        head = mk.commit("/", "-\"test2\"", head, null);
        head = mk.commit("/test", "+\"test\" : {\"id\":\"3\"}", head, null);

        Assert.assertEquals("/test/test", index.getPath("3", head));

        // Recreate the indexer
        indexer = new Indexer(mk);
        indexer.init();
        index = indexer.createPropertyIndex("id", true);
        head = mk.getHeadRevision();
        Assert.assertEquals("/test/test", index.getPath("3", head));
        Assert.assertEquals(null, index.getPath("0", head));

        Assert.assertEquals("/test", index.getPath("1", head));
        head = mk.commit("/", "^ \"test/id\": 100", head, null);
        Assert.assertEquals(null, index.getPath("1", head));
        Assert.assertEquals("/test", index.getPath("100", head));

        Assert.assertEquals("/test/test", index.getPath("3", head));
        head = mk.commit("/", "- \"test\"", head, null);
        Assert.assertEquals(null, index.getPath("100", head));
        Assert.assertEquals(null, index.getPath("3", head));
    }

}
