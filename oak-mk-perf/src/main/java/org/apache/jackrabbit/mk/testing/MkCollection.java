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

import java.util.ArrayList;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.util.Configuration;

/**
 * Represents a collection of microkernels.
 * 
 * @author rogoz
 * 
 */
public class MkCollection {
	ArrayList<MicroKernel> mks;

	/**
	 * Initialize a collection of mks.Each mk can have a particular
	 * configuration.
	 * 
	 * @param initializator
	 * @param conf
	 * @throws Exception
	 */
	public MkCollection(Initializator initializator, Configuration conf[],
			int size) throws Exception {
		mks = new ArrayList<MicroKernel>();
		for (int i = 0; i < size; i++) {
			mks.add(initializator.init(conf[i]));
		}
	}

	/**
	 * Initialize a collection of mks.All mks have the same configuration.
	 * 
	 * @param initializator
	 * @param conf
	 * @throws Exception
	 */
	public MkCollection(Initializator initializator, Configuration conf,
			int size) throws Exception {
		mks = new ArrayList<MicroKernel>();
		for (int i = 0; i < size; i++) {
			mks.add(initializator.init(conf));
		}
	}

	public ArrayList<MicroKernel> getMicroKernels() {
		return mks;
	}
}
