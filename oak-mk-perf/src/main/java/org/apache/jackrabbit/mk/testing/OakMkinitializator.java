package org.apache.jackrabbit.mk.testing;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.core.Repository;
import org.apache.jackrabbit.mk.util.Configuration;




public class OakMkinitializator implements Initializator {
	
	


	public MicroKernel init(Configuration conf) throws Exception {
		//TODO use configuration
		Repository rep = new Repository(conf.getStoragePath()+System.currentTimeMillis());
		rep.init();
		MicroKernel mk = new MicroKernelImpl(rep);
		return mk;
	}

	public String getType() {
		// TODO Auto-generated method stub
		return "Oak Microkernel";
	}

}
