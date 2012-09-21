package org.apache.jackrabbit.mk.tests;

import org.apache.jackrabbit.mk.util.MKOperation;
import org.apache.jackrabbit.mk.testing.MicroKernelTestBase;
import org.apache.jackrabbit.mk.util.Committer;
import org.junit.Test;

/**
 * Measure the time needed for writing nodes in different tree structures.All
 * the nodes are added in a single commit.
 * 
 * @author rogoz
 * 
 */
public class MkAddNodesDifferentStructuresTest extends MicroKernelTestBase {

	static int nodesNumber = 100000;
	static String nodeNamePrefix = "N";

	@Test
	public void testWriteNodesSameLevel() {

		String diff = MKOperation.buildPyramidDiff("/", 0, 0, nodesNumber,
				nodeNamePrefix, new StringBuilder()).toString();
		Committer committer = new Committer();
		chronometer.start();
		committer.addNodes(mk, diff, 0);
		chronometer.stop();
		System.out.println("Total time for testWriteNodesSameLevel is "
				+ chronometer.getSeconds());
	}

	@Test
	public void testWriteNodes1Child() {
		int nodesNumber = 100;

		String diff = MKOperation.buildPyramidDiff("/", 0, 1, nodesNumber,
				nodeNamePrefix, new StringBuilder()).toString();
		Committer committer = new Committer();
		chronometer.start();
		committer.addNodes(mk, diff, 0);
		chronometer.stop();
		System.out.println("Total time for testWriteNodes1Child is "
				+ chronometer.getSeconds());
	}

	@Test
	public void testWriteNodes10Children() {

		String diff = MKOperation.buildPyramidDiff("/", 0, 10, nodesNumber,
				nodeNamePrefix, new StringBuilder()).toString();
		Committer committer = new Committer();
		chronometer.start();
		committer.addNodes(mk, diff, 0);
		chronometer.stop();
		System.out.println("Total time for testWriteNodes10Children is "
				+ chronometer.getSeconds());
	}

	@Test
	public void testWriteNodes100Children() {

		String diff = MKOperation.buildPyramidDiff("/", 0, 100, nodesNumber,
				nodeNamePrefix, new StringBuilder()).toString();
		Committer committer = new Committer();
		chronometer.start();
		committer.addNodes(mk, diff, 0);
		chronometer.stop();
		System.out.println("Total time for testWriteNodes100Children is "
				+ chronometer.getSeconds());
	}

	@Test
	public void testWriteNodes1000Children() {
		String diff = MKOperation.buildPyramidDiff("/", 0, 1000, nodesNumber,
				nodeNamePrefix, new StringBuilder()).toString();
		Committer committer = new Committer();
		chronometer.start();
		committer.addNodes(mk, diff, 0);
		chronometer.stop();
		System.out.println("Total time for testWriteNodes1000Children is "
				+ chronometer.getSeconds());
	}
}
