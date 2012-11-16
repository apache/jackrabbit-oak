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

/**
 * Useful methods for building node structure.
 * 
 * 
 */
public class MicroKernelOperation {

    /**
     * Builds a diff representing a pyramid node structure.
     * 
     * @param The
     *            path where the first node will be added.
     * @param index
     * @param numberOfChildren
     *            The number of children that each node must have.
     * @param nodesNumber
     *            Total number of nodes.
     * @param nodePrefixName
     *            The node name prefix.
     * @param diff
     *            The string where the diff is builded.Put an empty string for
     *            creating a new structure.
     * @return
     */
    public static StringBuilder buildPyramidDiff(String startingPoint,
            int index, int numberOfChildren, long nodesNumber,
            String nodePrefixName, StringBuilder diff) {
        if (numberOfChildren == 0) {
            for (long i = 0; i < nodesNumber; i++)
                diff.append(addNodeToDiff(startingPoint, nodePrefixName + i));
            return diff;
        }
        if (index >= nodesNumber)
            return diff;
        diff.append(addNodeToDiff(startingPoint, nodePrefixName + index));
        // System.out.println("Create node "+ index);
        for (int i = 1; i <= numberOfChildren; i++) {
            if (!startingPoint.endsWith("/"))
                startingPoint = startingPoint + "/";
            buildPyramidDiff(startingPoint + nodePrefixName + index, index
                    * numberOfChildren + i, numberOfChildren, nodesNumber,
                    nodePrefixName, diff);
        }
        return diff;
    }

    private static String addNodeToDiff(String startingPoint, String nodeName) {
        if (!startingPoint.endsWith("/"))
            startingPoint = startingPoint + "/";

        return ("+\"" + startingPoint + nodeName + "\" : {\"key\":\"00000000000000000000\"} \n");
    }
}
