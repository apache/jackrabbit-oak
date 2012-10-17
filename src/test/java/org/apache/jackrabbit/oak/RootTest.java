/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidator;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Contains tests related to {@link Root}
 */
public class RootTest extends AbstractOakTest {

    @Override
    protected ContentRepository createRepository() {
        return new Oak()
                .with(new OpenSecurityProvider())
                .with(new ConflictValidator())
                .with(new AnnotatingConflictHandler())
                .createContentRepository();
    }

    @Test
    @Ignore("OAK-169")
    public void copyOrderableNodes() throws Exception {
        ContentSession s = createAdminSession();
        try {
            Root r = s.getLatestRoot();
            Tree t = r.getTree("/");
            Tree c = t.addChild("c");
            c.addChild("node1").orderBefore(null);
            c.addChild("node2");
            t.addChild("node3");
            r.commit();

            r.copy("/node3", "/c/node3");
            c = r.getTree("/").getChild("c");
            checkSequence(c.getChildren(), "node1", "node2", "node3");

        } finally {
            s.close();
        }
    }

    @Test
    @Ignore("OAK-169")
    public void moveOrderableNodes() throws Exception {
        ContentSession s = createAdminSession();
        try {
            Root r = s.getLatestRoot();
            Tree t = r.getTree("/");
            Tree c = t.addChild("c");
            c.addChild("node1").orderBefore(null);
            c.addChild("node2");
            t.addChild("node3");
            r.commit();

            r.move("/node3", "/c/node3");
            c = r.getTree("/").getChild("c");
            checkSequence(c.getChildren(), "node1", "node2", "node3");

        } finally {
            s.close();
        }
    }
}
