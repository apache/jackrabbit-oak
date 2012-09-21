package org.apache.jackrabbit.mk.util;

import org.apache.jackrabbit.mk.api.MicroKernel;

public class Committer {

	/**
	 * Add nodes to the repository.
	 * 
	 * @param mk
	 *            Microkernel that is performing the action.
	 * @param diff
	 *            The diff that is commited.All the nodes must have the absolute
	 *            path.
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
	}

	/**
	 * Add a node to repository.
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
}
