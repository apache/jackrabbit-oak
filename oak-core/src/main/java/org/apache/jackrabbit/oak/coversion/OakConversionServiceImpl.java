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

package org.apache.jackrabbit.oak.coversion;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.conversion.OakConversionService;
import org.apache.jackrabbit.oak.spi.adapter.AdapterManager;

import javax.annotation.Nonnull;

/**
 * Exposed OSGi service implementing the OakConversionService.
 * This is the service that components outside Oak should
 */
@Component( immediate = true)
@Service(OakConversionService.class)
public class OakConversionServiceImpl implements OakConversionService {


    @Reference
    private AdapterManager adapterManager;


    public OakConversionServiceImpl() {

    }

    /**
     * Non OSGi IoC constructor, requires and AdapterManager implementation.
     * @param adapterManager
     */
    public OakConversionServiceImpl(@Nonnull AdapterManager adapterManager) {
        this.adapterManager = adapterManager;
    }

    @Override
    public <T> T convertTo(Object source, Class<T> targetClass) {
        return adapterManager.adaptTo(source, targetClass);
    }
}
