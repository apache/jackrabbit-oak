/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.remote.content;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.remote.RemoteCommitException;
import org.junit.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class UnsetContentRemoteOperationTest {

    private UnsetContentRemoteOperation createOperation(String path, String name) {
        return new UnsetContentRemoteOperation(path, name);
    }

    @Test
    public void testUnset() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();
        doReturn(true).when(tree).hasProperty("name");

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name").apply(root);

        verify(tree).removeProperty("name");
    }

    @Test(expected = RemoteCommitException.class)
    public void testUnsetWithNonExistingTree() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(false).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name").apply(root);
    }

    @Test(expected = RemoteCommitException.class)
    public void testUnsetWithNonExistingProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();
        doReturn(false).when(tree).hasProperty("name");

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name").apply(root);
    }

}
