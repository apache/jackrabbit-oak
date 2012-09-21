package org.apache.jackrabbit.mk.testing;

import java.util.ArrayList;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.util.Configuration;



public class MkCollection {
	ArrayList<MicroKernel> mks;

	/**
	 * Initialize a collection of mks.Each mk can have a particular configuration.
	 * @param initializator
	 * @param conf
	 * @throws Exception
	 */
	public MkCollection(Initializator initializator,
			Configuration conf[], int size) throws Exception {
		mks = new ArrayList<MicroKernel>();
		for (int i = 0; i < size; i++) {
			mks.add(initializator.init(conf[i]));
		}
	}
	/**
	 * Initialize a collection of mks.All mks have the same configuration.
	 * @param initializator
	 * @param conf
	 * @throws Exception
	 */
	public MkCollection(Initializator initializator,
			Configuration conf,int size) throws Exception {
		mks=new ArrayList<MicroKernel>();
		for (int i = 0; i < size; i++) {
			mks.add(initializator.init(conf));
		}
	}
	public ArrayList<MicroKernel> getMicroKernels(){
		return mks;
	}
}
