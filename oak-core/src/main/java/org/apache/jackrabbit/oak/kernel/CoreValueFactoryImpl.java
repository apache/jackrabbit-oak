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
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.plugins.memory.MemoryValueFactory;

/**
 * {@code CoreValueFactoryImpl} is the default implementation of the
 * {@code CoreValueFactory} interface.
 */
public class CoreValueFactoryImpl extends MemoryValueFactory {

    private final MicroKernel mk;

    // TODO: currently public for query tests -> see todo there...
    public CoreValueFactoryImpl(MicroKernel mk) {
        this.mk = mk;
    }

    //---------------------------------------------------< CoreValueFactory >---

    @Override
    public CoreValue createValue(InputStream value) {
        // TODO: mk.write throws MicrokernelException ... deal with this here.
        String binaryID = mk.write(value);
        return new BinaryValue(binaryID, mk);
    }

    @Override
    public CoreValue createValue(String value, int type) {
        if (type == PropertyType.BINARY) {
            return new BinaryValue(value, mk);
        } else {
            return super.createValue(value, type);
        }
    }

}