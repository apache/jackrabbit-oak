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
package org.apache.jackrabbit.oak.kernel;

import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.util.CoreValueUtil;

public class KernelNodeStateBuilder implements NodeStateBuilder {
    private final MicroKernel kernel;
    private final CoreValueFactory valueFactory;
    private final String path;
    private final String[] revision;

    private KernelNodeStateBuilder(MicroKernel kernel, CoreValueFactory valueFactory, String path,
            String[] revision) {

        this.kernel = kernel;
        this.valueFactory = valueFactory;
        this.path = path;
        this.revision = revision;
    }
    
    public static NodeStateBuilder create(MicroKernel kernel, CoreValueFactory valueFactory,
            String path, String revision) {

        return new KernelNodeStateBuilder(kernel, valueFactory, path, new String[] {revision});
    }

    @Override
    public NodeState getNodeState() {
        assertNotStale();

        return new KernelNodeState(kernel, valueFactory, path, revision[0]);
    }

    @Override
    public NodeStateBuilder getChildBuilder(String name) {
        String targetPath = PathUtils.concat(path, name);
        return kernel.nodeExists(targetPath, revision[0])
            ? new KernelNodeStateBuilder(kernel, valueFactory, targetPath, revision)
            : null;
    }

    @Override
    public NodeStateBuilder addNode(String name, NodeState nodeState) {
        String targetPath = PathUtils.concat(path, name);
        StringBuilder jsop = new StringBuilder();
        buildJsop(targetPath, nodeState, jsop);
        revision[0] = kernel.commit("", jsop.toString(), revision[0], null);
        return new KernelNodeStateBuilder(kernel, valueFactory, targetPath, revision);
    }

    @Override
    public NodeStateBuilder addNode(String name) {
        String targetPath = PathUtils.concat(path, name);
        if (kernel.nodeExists(targetPath, revision[0])) {
            return null;
        }
        else {
            revision[0] = kernel.commit("", "+\""  + targetPath + "\":{}", revision[0], null);
            return new KernelNodeStateBuilder(kernel, valueFactory, targetPath, revision);
        }
    }

    @Override
    public boolean removeNode(String name) {
        String targetPath = PathUtils.concat(path, name);
        if (kernel.nodeExists(targetPath, revision[0])) {
            revision[0] = kernel.commit("", "-\""  + targetPath + '"', revision[0], null);
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public void setProperty(String name, CoreValue value) {
        String targetPath = PathUtils.concat(path, name);
        String json = CoreValueUtil.toJsonValue(value);

        revision[0] = kernel.commit("", "^\"" + targetPath + "\":" + json, revision[0], null);
    }

    @Override
    public void setProperty(String name, List<CoreValue> values) {
        String targetPath = PathUtils.concat(path, name);
        String json = CoreValueUtil.toJsonArray(values);

        revision[0] = kernel.commit("", "^\"" + targetPath + "\":" + json, revision[0], null);
    }

    @Override
    public void removeProperty(String name) {
        String targetPath = PathUtils.concat(path, name);
        revision[0] = kernel.commit("", "^\"" + targetPath + "\":null", revision[0], null);
    }

    @Override
    public boolean moveTo(NodeStateBuilder destParent, String destName) {
        if (!(destParent instanceof KernelNodeStateBuilder)) {
            throw new IllegalArgumentException("Alien builder for destParent");
        }

        if (destParent.getChildBuilder(destName) != null) {
            return false;
        }

        KernelNodeStateBuilder destParentBuilder = (KernelNodeStateBuilder) destParent;

        String destParentPath = destParentBuilder.getPath();
        String targetPath = PathUtils.concat(destParentPath, destName);

        revision[0] = kernel.commit("", ">\"" + path + "\":\"" + targetPath + '"',
                revision[0], null);
        return true;
    }

    @Override
    public boolean copyTo(NodeStateBuilder destParent, String destName) {
        if (!(destParent instanceof KernelNodeStateBuilder)) {
            throw new IllegalArgumentException("Alien builder for destParent");
        }

        if (destParent.getChildBuilder(destName) != null) {
            return false;
        }

        KernelNodeStateBuilder destParentBuilder = (KernelNodeStateBuilder) destParent;

        String destParentPath = destParentBuilder.getPath();
        String targetPath = PathUtils.concat(destParentPath, destName);

        revision[0] = kernel.commit("", "*\"" + path + "\":\"" + targetPath + '"',
                revision[0], null);
        return true;
    }

    //------------------------------------------------------------< internal >---

    String getPath() {
        return path;
    }

    void apply() throws CommitFailedException {
        assertNotStale();

        try {
            kernel.merge(revision[0], null);
            revision[0] = null;
        }
        catch (MicroKernelException e) {
            throw new CommitFailedException(e);
        }
    }

    //------------------------------------------------------------< private >---

    private void assertNotStale() {
        if (revision[0] == null) {
            throw new IllegalStateException("This branch has been merged already");
        }
    }

    private static void buildJsop(String path, NodeState nodeState, StringBuilder jsop) {
        jsop.append("+\"").append(path).append("\":{}");

        for (PropertyState property : nodeState.getProperties()) {
            String targetPath = PathUtils.concat(path, property.getName());
            String value = property.isArray()
                ? CoreValueUtil.toJsonArray(property.getValues())
                : CoreValueUtil.toJsonValue(property.getValue());

            jsop.append("^\"").append(targetPath).append("\":").append(value);
        }

        for (ChildNodeEntry child : nodeState.getChildNodeEntries(0, -1)) {
            String targetPath = PathUtils.concat(path, child.getName());
            buildJsop(targetPath, child.getNodeState(), jsop);
        }
    }

}
