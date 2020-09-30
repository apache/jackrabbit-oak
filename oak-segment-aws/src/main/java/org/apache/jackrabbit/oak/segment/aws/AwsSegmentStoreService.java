/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.aws;

import java.io.IOException;
import java.util.Properties;

import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE, configurationPid = { Configuration.PID })
public class AwsSegmentStoreService {

    public static final String DEFAULT_ROOT_DIRECTORY = "oak/";

    public static final String DEFAULT_JOURNALTABLE_NAME = "oakjournaltable";

    public static final String DEFAULT_LOCKTABLE_NAME = "oaklocktable";

    public static final String DEFAULT_REGION_NAME = "us-west-2";

    private ServiceRegistration registration;

    private SegmentNodeStorePersistence persistence;

    @Activate
    public void activate(ComponentContext context, Configuration config) throws IOException {
        persistence = createAwsPersistence(config);
        registration = context.getBundleContext().registerService(SegmentNodeStorePersistence.class.getName(),
                persistence, new Properties());
    }

    @Deactivate
    public void deactivate() throws IOException {
        if (registration != null) {
            registration.unregister();
            registration = null;
        }
        persistence = null;
    }

    private static SegmentNodeStorePersistence createAwsPersistence(Configuration configuration) throws IOException {
        AwsContext awsContext = AwsContext.create(configuration);
        AwsPersistence persistence = new AwsPersistence(awsContext);
        return persistence;
    }
}
