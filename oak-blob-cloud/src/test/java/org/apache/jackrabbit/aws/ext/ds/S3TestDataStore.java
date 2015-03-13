/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.aws.ext.ds;

import java.util.Properties;

import org.apache.jackrabbit.core.data.Backend;

/**
 * This class intialize {@link S3DataStore} with the give bucket. The other
 * configuration are taken from configuration file. This class is implemented so
 * that each test case run in its own bucket. It was required as deletions in
 * bucket are not immediately reflected in the next test case.
 */
public class S3TestDataStore extends S3DataStore {

    Properties props;

    public S3TestDataStore() {
        super();
    }

    public S3TestDataStore(Properties props) {
        super();
        this.props = props;
    }

    protected Backend createBackend() {
        Backend backend = new S3Backend();
        ((S3Backend) backend).setProperties(props);
        return backend;
    }
}
