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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import java.util.Random;

public final class ElasticTestUtils {

    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    public static final String ELASTIC_CONNECTION_STRING = System.getProperty("elasticConnectionString");

    public static final String ELASTIC_DOCKER_IMAGE_VERSION = System.getProperty("elasticDockerImageVersion");
    public static final String ELASTIC_KNN_PLUGIN_URI_KEY = "elasticKnnPluginUri";
    public static final String ELASTIC_KNN_PLUGIN_URI = System.getProperty(ELASTIC_KNN_PLUGIN_URI_KEY);

    public static String randomString(int size) {
        return randomString(new Random(42), size);
    }

    public static String randomString(Random random, int size) {
        int leftLimit = 48; // '0'
        int rightLimit = 122; // char '~'

        return random.ints(leftLimit, rightLimit + 1)
                .limit(size)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}
