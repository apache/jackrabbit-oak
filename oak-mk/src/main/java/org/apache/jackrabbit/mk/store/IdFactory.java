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
package org.apache.jackrabbit.mk.store;

import java.security.MessageDigest;

/**
 * Create new internal content object ids based on serialized data.
 */
public abstract class IdFactory {

    /**
     * Creates a new id based on the specified serialized data.
     * <p/>
     * The general contract of {@code createContentId} is:
     * <p/>
     * {@code createId(data1).equals(createId(data2)) == Arrays.equals(data1, data2)}
     *
     * @param serialized serialized data
     * @return raw node id as byte array
     * @throws Exception if an error occurs
     */
    public byte[] createContentId(byte[] serialized) throws Exception {
        return digest(serialized);
    }

    /**
     * Return a digest for some data.
     * 
     * @param data data
     * @return digest
     */
    protected byte[] digest(byte[] data) throws Exception {
        return MessageDigest.getInstance("SHA-1").digest(data);
    }
    
    /**
     * Return the default factory that will create node and revision ids based
     * on their content. 
     * 
     * @return factory
     */
    public static IdFactory getDigestFactory() {
        return new IdFactory() {};
    }
}
