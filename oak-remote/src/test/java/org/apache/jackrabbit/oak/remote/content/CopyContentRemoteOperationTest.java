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

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CopyContentRemoteOperationTest {

    CopyContentRemoteOperation createOperation(String source, String target) {
        return new CopyContentRemoteOperation(source, target);
    }

    @Test
    public void testCopy() throws Exception {
        Tree parent = mock(Tree.class);
        doReturn(true).when(parent).exists();

        Tree target = mock(Tree.class);
        doReturn(false).when(target).exists();
        doReturn(parent).when(target).getParent();
        doReturn("target").when(target).getName();

        Tree source = mock(Tree.class);
        doReturn(true).when(source).exists();
        doReturn(emptyList()).when(source).getProperties();
        doReturn(emptyList()).when(source).getChildren();

        Root root = mock(Root.class);
        doReturn(source).when(root).getTree("/source");
        doReturn(target).when(root).getTree("/target");

        createOperation("/source", "/target").apply(root);

        verify(parent).addChild("target");
    }

    @Test(expected = RemoteCommitException.class)
    public void testCopyWithNonExistingSource() throws Exception {
        Tree source = mock(Tree.class);
        doReturn(false).when(source).exists();

        Root root = mock(Root.class);
        doReturn(source).when(root).getTree("/source");

        createOperation("/source", "/target").apply(root);
    }

    @Test(expected = RemoteCommitException.class)
    public void testCopyWithExistingTarget() throws Exception {
        Tree target = mock(Tree.class);
        doReturn(true).when(target).exists();

        Tree source = mock(Tree.class);
        doReturn(true).when(source).exists();

        Root root = mock(Root.class);
        doReturn(source).when(root).getTree("/source");
        doReturn(target).when(root).getTree("/target");

        createOperation("/source", "/target").apply(root);
    }

    @Test(expected = RemoteCommitException.class)
    public void testCopyWithNonExistingTargetParent() throws Exception {
        Tree parent = mock(Tree.class);
        doReturn(false).when(parent).exists();

        Tree target = mock(Tree.class);
        doReturn(false).when(target).exists();
        doReturn(parent).when(target).getParent();

        Tree source = mock(Tree.class);
        doReturn(true).when(source).exists();

        Root root = mock(Root.class);
        doReturn(source).when(root).getTree("/source");
        doReturn(target).when(root).getTree("/target");

        createOperation("/source", "/target").apply(root);
    }

}
