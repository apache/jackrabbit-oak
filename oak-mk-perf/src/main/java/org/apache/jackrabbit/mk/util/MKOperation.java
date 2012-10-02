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

/**
 * Useful methods for building/committing nodes.
 * @author rogoz
 *
 */
public class MKOperation {

	/**
	 * Commit an empty node.
	 * 
	 * @param mk
	 *            The microkernel that performs the operation.
	 * @param parentNode
	 *            The path where the commit will be performed.
	 * @param name
	 *            Name of the node.
	 * @return
	 */
	private static String commitEmptyNode(MicroKernel mk, String parentNode,
			String name) {

		return mk.commit(parentNode, "+\"" + name + "\" : {} \n", null, "");
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
	public static void addPyramidStructure(MicroKernel mk,
			String startingPoint, int index, int numberOfChildren,
			long nodesNumber, String nodePrefixName) {
		// if all the nodes are on the same level
		if (numberOfChildren == 0) {
			for (long i = 0; i < nodesNumber; i++) {
				commitEmptyNode(mk, startingPoint, nodePrefixName + i);
				// System.out.println("Created node " + i);
			}
			return;
		}
		if (index >= nodesNumber)
			return;
		commitEmptyNode(mk, startingPoint, nodePrefixName + index);
		for (int i = 1; i <= numberOfChildren; i++) {
			if (!startingPoint.endsWith("/"))
				startingPoint = startingPoint + "/";
			addPyramidStructure(mk, startingPoint + nodePrefixName + index,
					index * numberOfChildren + i, numberOfChildren,
					nodesNumber, nodePrefixName);
		}

	}

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
			int index, int numberOfChildren, int nodesNumber,
			String nodePrefixName, StringBuilder diff) {
		if (numberOfChildren == 0) {
			for (long i = 0; i < nodesNumber; i++)
				diff.append(addNodeToDiff(startingPoint, nodePrefixName + i));
			return diff;
		}
		if (index >= nodesNumber)
			return diff;
		diff.append(addNodeToDiff(startingPoint, nodePrefixName + index));
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

		return ("+\"" + startingPoint + nodeName + "\" : {} \n");
	}
}
