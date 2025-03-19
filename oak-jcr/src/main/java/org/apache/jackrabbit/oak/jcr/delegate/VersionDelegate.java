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
package org.apache.jackrabbit.oak.jcr.delegate;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PREDECESSORS;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@code VersionDelegate}...
 */
public class VersionDelegate extends NodeDelegate {

    private VersionDelegate(SessionDelegate sessionDelegate, Tree tree) {
        super(requireNonNull(sessionDelegate), requireNonNull(tree));
    }

    static VersionDelegate create(@NotNull SessionDelegate sessionDelegate,
                                  @NotNull Tree tree) {
        return new VersionDelegate(sessionDelegate, tree);
    }

    @NotNull
    NodeDelegate getFrozenNode() throws RepositoryException {
        NodeDelegate frozenNode = getChild(JcrConstants.JCR_FROZENNODE);
        if (frozenNode == null) {
            throw new RepositoryException("Inconsistent version storage. " +
                    "Version at " + getPath() + " does not have a jcr:frozenNode");
        }
        return frozenNode;
    }

    @NotNull
    public Iterable<VersionDelegate> getPredecessors()
            throws RepositoryException {
        PropertyDelegate p = getPropertyOrNull(JCR_PREDECESSORS);
        if (p == null) {
            throw new RepositoryException("Inconsistent version storage. " +
                    "Version does not have a " + JCR_PREDECESSORS + " property.");
        }
        List<VersionDelegate> predecessors = new ArrayList<>();
        VersionManagerDelegate vMgr = VersionManagerDelegate.create(sessionDelegate);
        for (String id : p.getMultiState().getValue(Type.REFERENCES)) {
            predecessors.add(vMgr.getVersionByIdentifier(id));
        }
        return predecessors;
    }

    @Nullable
    public VersionDelegate getLinearPredecessor() throws RepositoryException {
        Iterable<VersionDelegate> predecessors = getPredecessors();
        if (predecessors.iterator().hasNext()) {
            // return first predecessor (same behavior as Jackrabbit)
            return predecessors.iterator().next();
        }
        return null;
    }
}
