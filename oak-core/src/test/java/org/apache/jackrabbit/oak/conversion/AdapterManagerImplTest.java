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


package org.apache.jackrabbit.oak.conversion;

import org.apache.jackrabbit.oak.coversion.AdapterManagerImpl;
import org.apache.jackrabbit.oak.coversion.OakConvertionServiceImpl;
import org.apache.jackrabbit.oak.spi.adapter.AdapterFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by boston on 30/08/2017.
 */
public class AdapterManagerImplTest {

    private AdapterManagerImpl adapterManager;
    private AdapterFactory adapterFactory1;
    private AdapterFactory adapterFactory2;

    @Before
    public void before() {
        adapterManager = new AdapterManagerImpl();
        adapterFactory1 = new AdapterFactory() {
            @Override
            public <T> T adaptTo(Object source, Class<T> targetClass) {
                if (source instanceof String) {
                    if (((String) source).startsWith("http") && URL.class.equals(targetClass)) {
                        try {
                            return (T) new URL((String) source);
                        } catch (MalformedURLException e) {
                            e.printStackTrace();
                        }
                    }
                    if ( URI.class.equals(targetClass)) {
                        try {
                            return (T) new URI((String) source);
                        } catch (URISyntaxException e) {
                            e.printStackTrace();
                        }
                    }
                }
                return null;
            }

            @Override
            public String[] getTargetClasses() {
                return new String[]{URI.class.getName(), URL.class.getName()};
            }

            @Override
            public int getPriority() {
                return 1;
            }
        };
        adapterFactory2 = new AdapterFactory() {
            @Override
            public <T> T adaptTo(Object source, Class<T> targetClass) {
                if ( source instanceof  String) {
                    if ( URI.class.equals(targetClass)) {
                        try {
                            return (T) new URI((String) source);
                        } catch (URISyntaxException e) {
                            e.printStackTrace();
                        }
                    }
                }
                return null;
            }

            @Override
            public String[] getTargetClasses() {
                return new String[]{ URI.class.getName()};
            }

            @Override
            public int getPriority() {
                return 2;
            }
        };

    }

    @Test
    public void testAdapterManagerImpl() {

        URI testing123_A0 = adapterManager.adaptTo("http://testing.com/123", URI.class);
        URI testing123_B0 = adapterManager.adaptTo("file://testing.com/123", URI.class);
        URL testing123_C0 = adapterManager.adaptTo("file://testing.com/123", URL.class);
        URL testing123_D0 = adapterManager.adaptTo("http://testing.com/123", URL.class);

        Assert.assertNull(testing123_A0);
        Assert.assertNull(testing123_B0);
        Assert.assertNull(testing123_C0);
        Assert.assertNull(testing123_D0);

        adapterManager.addAdapterFactory(adapterFactory1);
        adapterManager.addAdapterFactory(adapterFactory2);

        URI testing123_A = adapterManager.adaptTo("http://testing.com/123", URI.class);
        URI testing123_B = adapterManager.adaptTo("file://testing.com/123", URI.class);
        URL testing123_C = adapterManager.adaptTo("file://testing.com/123", URL.class);
        URL testing123_D = adapterManager.adaptTo("http://testing.com/123", URL.class);

        Assert.assertNotNull(testing123_A);
        Assert.assertEquals(testing123_A.getClass(),URI.class);
        Assert.assertNotNull(testing123_B);
        Assert.assertEquals(testing123_B.getClass(),URI.class);
        Assert.assertNull(testing123_C);
        Assert.assertNotNull(testing123_D);
        Assert.assertEquals(testing123_D.getClass(),URL.class);

        adapterManager.removeAdapterFactory(adapterFactory1);

        URI testing123_A1 = adapterManager.adaptTo("http://testing.com/123", URI.class);
        URI testing123_B1 = adapterManager.adaptTo("file://testing.com/123", URI.class);
        URL testing123_C1 = adapterManager.adaptTo("file://testing.com/123", URL.class);
        URL testing123_D1 = adapterManager.adaptTo("http://testing.com/123", URL.class);

        Assert.assertNotNull(testing123_A1);
        Assert.assertEquals(testing123_A1.getClass(),URI.class);
        Assert.assertNotNull(testing123_B1);
        Assert.assertEquals(testing123_B1.getClass(),URI.class);
        Assert.assertNull(testing123_C1);
        Assert.assertNull(testing123_D1);

        adapterManager.removeAdapterFactory(adapterFactory2);

        URI testing123_A2 = adapterManager.adaptTo("http://testing.com/123", URI.class);
        URI testing123_B2 = adapterManager.adaptTo("file://testing.com/123", URI.class);
        URL testing123_C2 = adapterManager.adaptTo("file://testing.com/123", URL.class);
        URL testing123_D2 = adapterManager.adaptTo("http://testing.com/123", URL.class);

        Assert.assertNull(testing123_A2);
        Assert.assertNull(testing123_B2);
        Assert.assertNull(testing123_C2);
        Assert.assertNull(testing123_D2);

    }

    @Test
    public void testOakConversionService() {
        OakConvertionServiceImpl oakConvertionService = new OakConvertionServiceImpl(adapterManager);

        URI testing123_A0 = oakConvertionService.convertTo("http://testing.com/123", URI.class);
        URI testing123_B0 = oakConvertionService.convertTo("file://testing.com/123", URI.class);
        URL testing123_C0 = oakConvertionService.convertTo("file://testing.com/123", URL.class);
        URL testing123_D0 = oakConvertionService.convertTo("http://testing.com/123", URL.class);

        Assert.assertNull(testing123_A0);
        Assert.assertNull(testing123_B0);
        Assert.assertNull(testing123_C0);
        Assert.assertNull(testing123_D0);

        adapterManager.addAdapterFactory(adapterFactory1);
        adapterManager.addAdapterFactory(adapterFactory2);

        URI testing123_A = oakConvertionService.convertTo("http://testing.com/123", URI.class);
        URI testing123_B = oakConvertionService.convertTo("file://testing.com/123", URI.class);
        URL testing123_C = oakConvertionService.convertTo("file://testing.com/123", URL.class);
        URL testing123_D = oakConvertionService.convertTo("http://testing.com/123", URL.class);

        Assert.assertNotNull(testing123_A);
        Assert.assertEquals(testing123_A.getClass(),URI.class);
        Assert.assertNotNull(testing123_B);
        Assert.assertEquals(testing123_B.getClass(),URI.class);
        Assert.assertNull(testing123_C);
        Assert.assertNotNull(testing123_D);
        Assert.assertEquals(testing123_D.getClass(),URL.class);

        adapterManager.removeAdapterFactory(adapterFactory1);

        URI testing123_A1 = oakConvertionService.convertTo("http://testing.com/123", URI.class);
        URI testing123_B1 = oakConvertionService.convertTo("file://testing.com/123", URI.class);
        URL testing123_C1 = oakConvertionService.convertTo("file://testing.com/123", URL.class);
        URL testing123_D1 = oakConvertionService.convertTo("http://testing.com/123", URL.class);

        Assert.assertNotNull(testing123_A1);
        Assert.assertEquals(testing123_A1.getClass(),URI.class);
        Assert.assertNotNull(testing123_B1);
        Assert.assertEquals(testing123_B1.getClass(),URI.class);
        Assert.assertNull(testing123_C1);
        Assert.assertNull(testing123_D1);

        adapterManager.removeAdapterFactory(adapterFactory2);

        URI testing123_A2 = oakConvertionService.convertTo("http://testing.com/123", URI.class);
        URI testing123_B2 = oakConvertionService.convertTo("file://testing.com/123", URI.class);
        URL testing123_C2 = oakConvertionService.convertTo("file://testing.com/123", URL.class);
        URL testing123_D2 = oakConvertionService.convertTo("http://testing.com/123", URL.class);

        Assert.assertNull(testing123_A2);
        Assert.assertNull(testing123_B2);
        Assert.assertNull(testing123_C2);
        Assert.assertNull(testing123_D2);


    }
}
