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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import javax.jcr.Node;

import org.apache.jackrabbit.JcrConstants;

/**
 * Permission evaluation tests for session move operations for referenceable nodes.
 */
public class SessionMoveReferenceableTest extends SessionMoveTest {

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Node n = superuser.getNode(childNPath);
        n.addMixin(JcrConstants.MIX_REFERENCEABLE);

        n = superuser.getNode(nodePath3);
        n.addMixin(JcrConstants.MIX_REFERENCEABLE);

        superuser.save();
    }
}
