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
package org.apache.jackrabbit.oak.plugins.commit;

import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.jackrabbit.oak.api.Type.DATE;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Assert;
import org.junit.Test;

public class JcrLastModifiedConflictHandlerTest {

    @Test
    public void updates() throws Exception {

        ContentRepository repo = newRepo(new JcrLastModifiedConflictHandler());
        Root root = login(repo);
        setup(root);

        Root ourRoot = login(repo);
        Root theirRoot = login(repo);

        PropertyState p0 = createDateProperty(JCR_CREATED);
        ourRoot.getTree("/c").setProperty(p0);

        TimeUnit.MILLISECONDS.sleep(50);
        theirRoot.getTree("/c").setProperty(createDateProperty(JCR_CREATED));

        ourRoot.commit();
        theirRoot.commit();

        root.refresh();
        Assert.assertEquals(p0, root.getTree("/c").getProperty(JCR_CREATED));
        TimeUnit.MILLISECONDS.sleep(50);

        ourRoot.refresh();
        theirRoot.refresh();

        ourRoot.getTree("/c").setProperty(createDateProperty(JCR_LASTMODIFIED));
        TimeUnit.MILLISECONDS.sleep(50);
        PropertyState p1 = createDateProperty(JCR_LASTMODIFIED);
        theirRoot.getTree("/c").setProperty(p1);

        ourRoot.commit();
        theirRoot.commit();

        root.refresh();
        Assert.assertEquals(p1, root.getTree("/c").getProperty(JCR_LASTMODIFIED));
    }

    public static PropertyState createDateProperty(@Nonnull String name) {
        String now = ISO8601.format(Calendar.getInstance());
        return PropertyStates.createProperty(name, now, DATE);
    }

    private static ContentRepository newRepo(ThreeWayConflictHandler handler) {
        return new Oak().with(new OpenSecurityProvider()).with(handler).createContentRepository();
    }

    private static Root login(ContentRepository repo) throws Exception {
        return repo.login(null, null).getLatestRoot();
    }

    private static void setup(Root root) throws Exception {
        Tree tree = root.getTree("/");
        tree.addChild("c");
        root.commit();
    }

}
