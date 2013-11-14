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
package org.apache.jackrabbit.oak.jcr.repository;

import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.core.DescriptorsImpl;

import static javax.jcr.Repository.OPTION_LOCKING_SUPPORTED;
import static javax.jcr.Repository.OPTION_XML_EXPORT_SUPPORTED;
import static javax.jcr.Repository.OPTION_XML_IMPORT_SUPPORTED;

/**
 * The {@code JcrDescriptorsImpl} extend the {@link DescriptorsImpl} by automatically marking some of the JCR
 * features as supported.
 */
public class JcrDescriptorsImpl extends DescriptorsImpl {

    public JcrDescriptorsImpl(Descriptors base, ValueFactory valueFactory) {
        super(base);

        // add the descriptors of the features that are provided by the JCR layer
        final Value trueValue = valueFactory.createValue(true);
        final Value falseValue = valueFactory.createValue(false);
        put(new DescriptorsImpl.Descriptor(
                OPTION_LOCKING_SUPPORTED,
                trueValue, true, true));
        put(new DescriptorsImpl.Descriptor(
                OPTION_XML_EXPORT_SUPPORTED,
                trueValue, true, true));
        put(new DescriptorsImpl.Descriptor(
                OPTION_XML_IMPORT_SUPPORTED,
                trueValue, true, true));
   }
}