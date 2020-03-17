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
package org.apache.jackrabbit.oak.commons.jmx;

import javax.management.DescriptorRead;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.StandardMBean;

import org.apache.jackrabbit.oak.api.jmx.Description;
import org.apache.jackrabbit.oak.api.jmx.Impact;
import org.apache.jackrabbit.oak.api.jmx.ImpactOption;
import org.apache.jackrabbit.oak.api.jmx.Name;

/**
 * The extension of {@link javax.management.StandardMBean} that will automatically provide JMX
 * metadata through annotations.
 *
 * @see javax.management.MBeanInfo
 * @see Description
 * @see Name
 * @see Impact
 */
public class AnnotatedStandardMBean extends StandardMBean {
    /**
     * Make a DynamicMBean out of the object implementation, using the specified
     * mbeanInterface class.
     *
     * @see javax.management.StandardMBean#StandardMBean(Object, Class)
     */
    public <T> AnnotatedStandardMBean(T implementation, Class<T> mbeanInterface){
        super(implementation, mbeanInterface, false);
    }

    protected AnnotatedStandardMBean(Class<?> mbeanInterface){
        super(mbeanInterface, false);
    }

    @Override
    protected String getDescription(MBeanInfo info) {
        String desc = getValue(info, Description.NAME);
        return desc == null ? super.getDescription(info) : desc;
    }

    @Override
    protected String getDescription(MBeanAttributeInfo info) {
        String desc = getValue(info, Description.NAME);
        return desc == null ? super.getDescription(info) : desc;
    }

    @Override
    protected String getDescription(MBeanOperationInfo info) {
        String desc = getValue(info, Description.NAME);
        return desc == null ? super.getDescription(info) : desc;
    }

    @Override
    protected int getImpact(MBeanOperationInfo info) {
        String opt = getValue(info, Impact.NAME);
        return opt == null ? super.getImpact(info) : ImpactOption.valueOf(opt).value();
    }

    @Override
    protected String getParameterName(MBeanOperationInfo op,
                                      MBeanParameterInfo param, int sequence) {
        String name = getValue(param, Name.NAME);
        return name == null
                ? super.getParameterName(op, param, sequence)
                : name;
    }

    @Override
    protected String getDescription(MBeanOperationInfo op,
                                    MBeanParameterInfo param, int sequence) {
        String desc = getValue(param, Description.NAME);
        return desc == null
                ? super.getDescription(op, param, sequence)
                : desc;
    }

    private static String getValue(DescriptorRead dr, String fieldName){
        return (String) dr.getDescriptor().getFieldValue(fieldName);
    }
}
