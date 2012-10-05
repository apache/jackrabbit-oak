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
package org.apache.jackrabbit.oak.kernel;

import java.io.InputStream;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.util.MicroKernelInputStream;
import org.apache.jackrabbit.oak.plugins.memory.MemoryValue;

/**
 * BinaryValue... TODO: review name (BlobValue? BlobCoreValue? BinaryCoreValue?)
 */
class BinaryValue extends MemoryValue {

    private final String binaryID;
    private final MicroKernel mk;

    public BinaryValue(String binaryID, MicroKernel mk) {
        this.binaryID = binaryID;
        this.mk = mk;
    }

    @Override
    public int getType() {
        return PropertyType.BINARY;
    }

    @Override
    public boolean getBoolean() {
        return Boolean.parseBoolean(getString());
    }

    @Override
    public InputStream getNewStream() {
        return new MicroKernelInputStream(mk, binaryID);
    }

    @Override
    public String getString() {
        return binaryID;
    }

    @Override
    public long length() {
        return mk.getLength(binaryID);
    }

    //-------------------------------------------------------------< Object >---

    @Override
    public int hashCode() {
        // TODO
        return binaryID.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (o instanceof BinaryValue) {
            BinaryValue other = (BinaryValue) o;
            return binaryID.equals(other.binaryID);
        }

        return super.equals(o);
    }

}