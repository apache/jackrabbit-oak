package org.apache.jackrabbit.mk.tests;

import org.apache.jackrabbit.mk.util.MKOperation;
import org.apache.jackrabbit.mk.testing.MicroKernelTestBase;
import org.junit.Test;

/**
 * Measure the time needed for writing nodes in different tree structures.Each
 * node is committed separately.Each node is committed using the relative path of
 * the parent node.
 * 
 * @author rogoz
 * 
 */

public class MKAddNodesRelativePathTest extends MicroKernelTestBase {

	static String nodeNamePrefix = "N";
	static int nodesNumber = 1000;

	/**
	 * Writes all the nodes on the same level.All the nodes have the same
	 * parent.
	 * 
	 * @throws Exception
	 */

	@Test
	public void testWriteNodesSameLevel() throws Exception {

		chronometer.start();
		MKOperation.addPyramidStructure(mk, "/", 0, 0, nodesNumber,
				nodeNamePrefix);
		chronometer.stop();
		System.out.println("Total time for testWriteNodesSameLevel is "
				+ chronometer.getSeconds());
	}

	@Test
	public void testWriteNodes10Children() {
		chronometer.start();
		MKOperation.addPyramidStructure(mk, "/", 0, 10, nodesNumber,
				nodeNamePrefix);
		chronometer.stop();
		System.out.println("Total time for testWriteNodes10Children is "
				+ chronometer.getSeconds());
	}

	@Test
	public void testWriteNodes100Children() {
		chronometer.start();
		MKOperation.addPyramidStructure(mk, "/", 0, 100, nodesNumber,
				nodeNamePrefix);
		chronometer.stop();
		System.out.println("Total time for testWriteNodes100Children is "
				+ chronometer.getSeconds());
	}
}
