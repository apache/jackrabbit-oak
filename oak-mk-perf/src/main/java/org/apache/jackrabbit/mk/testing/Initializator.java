package org.apache.jackrabbit.mk.testing;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.util.Configuration;


public interface Initializator {

	public MicroKernel init(Configuration conf) throws Exception;
	public String getType();
}
