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
package org.apache.jackrabbit.mk.util;

import org.apache.jackrabbit.mk.api.MicroKernel;

public class Committer {

    /**
     * Add nodes to the repository.
     * 
     * @param mk
     *            The microkernel that is performing the action.
     * @param diff
     *            The diff that is commited.All the nodes must be define by
     *            their absolute path.
     * @param nodesPerCommit
     *            Number of nodes per commit.
     */
    public void addNodes(MicroKernel mk, String diff, int nodesPerCommit) {

        if (nodesPerCommit == 0) {
            mk.commit("", diff.toString(), null, "");
            return;
        }
        String[] string = diff.split(System.getProperty("line.separator"));
        int i = 0;
        StringBuilder finalCommit = new StringBuilder();
        for (String line : string) {
            finalCommit.append(line);
            i++;
            if (i == nodesPerCommit) {
                mk.commit("", finalCommit.toString(), null, "");
                finalCommit.setLength(0);
                i = 0;
            }
        }
        if (finalCommit.length() > 0)
            mk.commit("", finalCommit.toString(), null, "");
    }

    /**
     * Add an empty node to repository.
     * 
     * @param mk
     *            Microkernel that is performing the action.
     * @param parentPath
     * @param name
     *            Name of the node.
     */
    public void addNode(MicroKernel mk, String parentPath, String name) {
        mk.commit(parentPath, "+\"" + name + "\" : {} \n", null, "");
    }

    /**
     * Recursively builds a pyramid tree structure.Each node is added in a
     * separate commit.
     * 
     * @param mk
     *            Microkernel used for adding nodes.
     * @param startingPoint
     *            The path where the node will be added.
     * @param index
     * @param numberOfChildren
     *            Number of children per level.
     * @param nodesNumber
     *            Total nodes number.
     * @param nodePrefixName
     *            The node's name prefix.The complete node name is
     *            prefix+indexNumber.
     **/
    public void addPyramidStructure(MicroKernel mk, String startingPoint,
            int index, int numberOfChildren, long nodesNumber,
            String nodePrefixName) {
        // if all the nodes are on the same level
        if (numberOfChildren == 0) {
            for (long i = 0; i < nodesNumber; i++) {
                addNode(mk, startingPoint, nodePrefixName + i);
                // System.out.println("Created node " + i);
            }
            return;
        }
        if (index >= nodesNumber)
            return;
        addNode(mk, startingPoint, nodePrefixName + index);
        for (int i = 1; i <= numberOfChildren; i++) {
            if (!startingPoint.endsWith("/"))
                startingPoint = startingPoint + "/";
            addPyramidStructure(mk, startingPoint + nodePrefixName + index,
                    index * numberOfChildren + i, numberOfChildren,
                    nodesNumber, nodePrefixName);
        }

    }
}
