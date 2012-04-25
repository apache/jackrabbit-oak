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
package org.apache.jackrabbit.oak.jcr.value;

/**
 * DummyNamePathResolver...
 * TODO: replace by reasonable implementation taking namespace mappings etc. into account.
 */
class DummyNamePathResolver {
    String getJCRPath(String internalPath) {
        return internalPath;
    }

    String getInternalPath(String jcrPath) {
        return jcrPath;
    }

    String getJCRName(String internalName) {
        return internalName;
    }

    String getInternalName(String jcrName) {
        return jcrName;
    }
}