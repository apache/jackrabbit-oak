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
import org.apache.jackrabbit.mk.util.Chronometer;
import org.apache.jackrabbit.mk.util.Configuration;
import org.apache.jackrabbit.mk.util.MicroKernelConfigProvider;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * The test base class for tests that are using only one microkernel instance.
 *
 *
 *
 */
public class MicroKernelTestBase {

    static MicroKernelInitializer initializator;
    public MicroKernel mk;
    public static Configuration conf;
    public Chronometer chronometer;

    /**
     * Loads the corresponding microkernel initialization class and the
     * microkernel configuration.The method searches for the <b>mk.type</b>
     * system property in order to initialize the proper microkernel.By default,
     * the oak microkernel will be instantiated.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void beforeSuite() throws Exception {

        // FIXME - Add back
        initializator = new OakMicroKernelInitializer();
        //String mktype = System.getProperty("mk.type");
        //initializator = (mktype == null || mktype.equals("oakmk")) ? new OakMicroKernelInitializer()
        //        : new MongoMicroKernelInitializer();
        System.out.println("Tests will run against ***"
                + initializator.getType() + "***");
        conf = MicroKernelConfigProvider.readConfig();
    }

    /**
     * Creates a microkernel collection with only one microkernel.
     *
     * @throws Exception
     */
    @Before
    public void beforeTest() throws Exception {

        mk = (new MicroKernelCollection(initializator, conf, 1))
                .getMicroKernels().get(0);
        chronometer = new Chronometer();
    }

}
