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
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.core.Repository;
import org.apache.jackrabbit.mk.util.Configuration;

/**
 * Initialize a {@code MicroKernelImpl}.A new {@code Repository} is created for
 * each initialization.
 */
public class OakMicroKernelInitializer implements MicroKernelInitializer {

    @Override
    public ArrayList<MicroKernel> init(Configuration conf, int mksNumber)
            throws Exception {
        ArrayList<MicroKernel> mks = new ArrayList<MicroKernel>();
        Repository rep = new Repository(conf.getStoragePath()
                + System.currentTimeMillis());
        rep.init();
        for (int i = 0; i < mksNumber; i++) {
            mks.add(new MicroKernelImpl(rep));
        }
        return mks;
    }

    public String getType() {
        // TODO Auto-generated method stub
        return "Oak Microkernel";
    }

}
