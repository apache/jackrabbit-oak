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
package org.apache.jackrabbit.mk.testing;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.core.Repository;
import org.apache.jackrabbit.mk.util.Configuration;

/**
 * Initialize an oak microkernel.
 * 
 * @author rogoz
 * 
 */
public class OakMkinitializator implements Initializator {

	public MicroKernel init(Configuration conf) throws Exception {
		// TODO use configuration
		Repository rep = new Repository(conf.getStoragePath()
				+ System.currentTimeMillis());
		rep.init();
		MicroKernel mk = new MicroKernelImpl(rep);
		return mk;
	}

	public String getType() {
		// TODO Auto-generated method stub
		return "Oak Microkernel";
	}

}
