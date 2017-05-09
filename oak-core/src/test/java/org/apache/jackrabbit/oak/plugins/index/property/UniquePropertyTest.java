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
package org.apache.jackrabbit.oak.plugins.index.property;

import java.util.UUID;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * UniquePropertyTest...
 */
public class UniquePropertyTest {

    @Test
    public void testUniqueness() throws Exception {

        Root root = new Oak()
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new InitialContent()).createRoot();

        NodeUtil node = new NodeUtil(root.getTree("/"));
        String uuid = UUID.randomUUID().toString();
        node.setString(JcrConstants.JCR_UUID, uuid);
        root.commit();

        NodeUtil child = new NodeUtil(root.getTree("/")).addChild("another", "rep:User");
        child.setString(JcrConstants.JCR_UUID, uuid);
        try {
            root.commit();
            fail("Duplicate jcr:uuid should be detected.");
        } catch (CommitFailedException e) {
            // expected
        }
    }

}