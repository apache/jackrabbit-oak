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
package org.apache.jackrabbit.mk.index;

import java.util.Iterator;
import junit.framework.Assert;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.plugins.index.Indexer;
import org.apache.jackrabbit.oak.plugins.index.PrefixIndex;
import org.junit.Test;

/**
 * Test the prefix index.
 */
public class PrefixIndexTest {

    @Test
    public void test() {
        MicroKernel mk = new MicroKernelImpl();
        Indexer indexer = Indexer.getInstance(mk);
        indexer.init();
        PrefixIndex index = indexer.createPrefixIndex("d:");

        String head = mk.getHeadRevision();

        // meta data
        String meta = mk.getNodes(Indexer.INDEX_CONFIG_PATH, head, 1, 0, -1, null);

        Assert.assertEquals("{\":childNodeCount\":2,\"prefix@d:\":" +
                "{\":childNodeCount\":1,\":data\":{}},\":data\":{\":childNodeCount\":0}}", meta);

        Assert.assertEquals("", getPathList(index, "d:1", head));

        head = mk.commit("/", "+\"test\" : {\"blob\":\"d:1\"}", head, null);
        head = mk.commit("/", "+\"test2\" : {\"blob2\":\"d:2\"}", head, null);

        Assert.assertEquals("/test/blob", getPathList(index, "d:1", head));
        Assert.assertEquals("/test2/blob2", getPathList(index, "d:2", head));

        head = mk.commit("/", "^\"test2/blob2\" : null", head, null);
        Assert.assertEquals("", getPathList(index, "d:2", head));

        head = mk.commit("/", "^\"test2/blob2\" : \"d:2\" ", head, null);
        Assert.assertEquals("/test2/blob2", getPathList(index, "d:2", head));

        head = mk.commit("/", "+\"test3\" : {\"blob3\":\"d:1\"}", head, null);
        head = mk.commit("/", "+\"test4\" : {\"blob4\":\"d:2\"}", head, null);

        Assert.assertEquals("/test/blob, /test3/blob3", getPathList(index, "d:1", head));
        Assert.assertEquals("/test2/blob2, /test4/blob4", getPathList(index, "d:2", head));

        head = mk.commit("/", "+\"test5\" : {\"blobs\":[\"a:0\",\"d:2\"]}", head, null);
        head = mk.commit("/", "+\"test6\" : {\"data\":[true, false, null, 1, -1]}", head, null);
        Assert.assertEquals("/test2/blob2, /test4/blob4, /test5/blobs", getPathList(index, "d:2", head));

        head = mk.commit("/", "+\"test7\" : {\"a\":\"d:4\", \"b\":\"d:4\"}", head, null);
        Assert.assertEquals("/test7/a, /test7/b", getPathList(index, "d:4", head));
        head = mk.commit("/", "^\"test7/a\" : null", head, null);
        Assert.assertEquals("/test7/b", getPathList(index, "d:4", head));
    }

    private static String getPathList(PrefixIndex index, String value, String revision) {
        StringBuilder buff = new StringBuilder();
        int i = 0;
        for (Iterator<String> it = index.getPaths(value, revision); it.hasNext();) {
            if (i++ > 0) {
                buff.append(", ");
            }
            buff.append(it.next());
        }
        return buff.toString();
    }

}
