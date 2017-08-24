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

import org.apache.jackrabbit.oak.api.conversion.OakConversionService;
import org.apache.jackrabbit.oak.spi.adapter.AdapterManager;
// import org.osgi.service.component.annotations.Component;

/**
 * Exposed OSGi service implementing the OakConversionService.
 * Commented out the Component annotation since oak-core doesnt depend on OSGi, not certain where this should be.
 * TODO: move the a better location where OSGi is valid.
 */
// @Component(service = OakConversionService.class, immediate = true)
public class OakConvertionServiceImpl implements OakConversionService {

    @Override
    public <T> T convertTo(Object source, Class<T> targetClass) {
        return AdapterManager.getInstance().getAdapter(source, targetClass);
    }
}
