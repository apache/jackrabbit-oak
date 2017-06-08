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

package org.apache.jackrabbit.oak.osgi;

import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.component.ComponentContext;

import java.util.Dictionary;
import java.util.Map;

import static com.google.common.collect.Maps.newLinkedHashMap;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.appendEscapedLdapValue;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.appendLdapFilterAttribute;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.getFilter;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookup;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupConfigurationThenFramework;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupFrameworkThenConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class OsgiUtilTest {

    @Test(expected = NullPointerException.class)
    public void testComponentLookupWithNullContext() {
        lookup((ComponentContext) null, "name");
    }

    @Test(expected = NullPointerException.class)
    public void testComponentLookupWithNullName() {
        lookup(mock(ComponentContext.class), null);
    }

    @Test
    public void testComponentLookupWithNotFoundValue() {
        Dictionary properties = mock(Dictionary.class);
        doReturn(null).when(properties).get("name");

        ComponentContext context = mock(ComponentContext.class);
        doReturn(properties).when(context).getProperties();

        assertNull(lookup(context, "name"));
    }

    @Test
    public void testComponentLookupWithEmptyString() {
        Dictionary properties = mock(Dictionary.class);
        doReturn("").when(properties).get("name");

        ComponentContext context = mock(ComponentContext.class);
        doReturn(properties).when(context).getProperties();

        assertNull(lookup(context, "name"));
    }

    @Test
    public void testComponentLookupWithNonTrimmedString() {
        Dictionary properties = mock(Dictionary.class);
        doReturn("   ").when(properties).get("name");

        ComponentContext context = mock(ComponentContext.class);
        doReturn(properties).when(context).getProperties();

        assertNull(lookup(context, "name"));
    }

    @Test
    public void testComponentLookupWithNonStringValue() {
        Dictionary properties = mock(Dictionary.class);
        doReturn(42).when(properties).get("name");

        ComponentContext context = mock(ComponentContext.class);
        doReturn(properties).when(context).getProperties();

        assertEquals("42", lookup(context, "name"));
    }

    @Test
    public void testComponentLookupWithStringValue() {
        Dictionary properties = mock(Dictionary.class);
        doReturn("  value   ").when(properties).get("name");

        ComponentContext context = mock(ComponentContext.class);
        doReturn(properties).when(context).getProperties();

        assertEquals("value", lookup(context, "name"));
    }

    @Test(expected = NullPointerException.class)
    public void testFrameworkLookupWithNullContext() {
        lookup((BundleContext) null, "name");
    }

    @Test(expected = NullPointerException.class)
    public void testFrameworkLookupWithNullName() {
        lookup(mock(BundleContext.class), null);
    }

    @Test
    public void testFrameworkLookupWithNotFoundValue() {
        BundleContext context = mock(BundleContext.class);
        doReturn(null).when(context).getProperty("name");

        assertNull(lookup(context, "name"));
    }

    @Test
    public void testFrameworkLookupWithEmptyString() {
        BundleContext context = mock(BundleContext.class);
        doReturn("").when(context).getProperty("name");

        assertNull(lookup(context, "name"));
    }

    @Test
    public void testFrameworkLookupWithNonTrimmedString() {
        BundleContext context = mock(BundleContext.class);
        doReturn("   ").when(context).getProperty("name");

        assertNull(lookup(context, "name"));
    }

    @Test
    public void testFrameworkLookupWith() {
        BundleContext context = mock(BundleContext.class);
        doReturn("  value   ").when(context).getProperty("name");

        assertEquals("value", lookup(context, "name"));
    }

    @Test
    public void testFallbackLookupWithNoValue() {
        Dictionary dictionary = mock(Dictionary.class);
        doReturn(null).when(dictionary).get("cname");

        BundleContext bundleContext = mock(BundleContext.class);
        doReturn(null).when(bundleContext).getProperty("fname");

        ComponentContext componentContext = mock(ComponentContext.class);
        doReturn(dictionary).when(componentContext).getProperties();
        doReturn(bundleContext).when(componentContext).getBundleContext();

        assertNull(lookupConfigurationThenFramework(componentContext, "cname", "fname"));
        assertNull(lookupFrameworkThenConfiguration(componentContext, "cname", "fname"));
    }

    @Test
    public void testFallbackLookupWithValueInComponent() {
        Dictionary dictionary = mock(Dictionary.class);
        doReturn("value").when(dictionary).get("cname");

        BundleContext bundleContext = mock(BundleContext.class);
        doReturn(null).when(bundleContext).getProperty("fname");

        ComponentContext componentContext = mock(ComponentContext.class);
        doReturn(dictionary).when(componentContext).getProperties();
        doReturn(bundleContext).when(componentContext).getBundleContext();

        assertEquals("value", lookupConfigurationThenFramework(componentContext, "cname", "fname"));
        assertEquals("value", lookupFrameworkThenConfiguration(componentContext, "cname", "fname"));
    }

    @Test
    public void testFallbackLookupWithValueInFramework() {
        Dictionary dictionary = mock(Dictionary.class);
        doReturn(null).when(dictionary).get("cname");

        BundleContext bundleContext = mock(BundleContext.class);
        doReturn("value").when(bundleContext).getProperty("fname");

        ComponentContext componentContext = mock(ComponentContext.class);
        doReturn(dictionary).when(componentContext).getProperties();
        doReturn(bundleContext).when(componentContext).getBundleContext();

        assertEquals("value", lookupConfigurationThenFramework(componentContext, "cname", "fname"));
        assertEquals("value", lookupFrameworkThenConfiguration(componentContext, "cname", "fname"));
    }

    @Test
    public void testFallbackLookupWithValueInComponentAndFramework() {
        Dictionary dictionary = mock(Dictionary.class);
        doReturn("cvalue").when(dictionary).get("cname");

        BundleContext bundleContext = mock(BundleContext.class);
        doReturn("fvalue").when(bundleContext).getProperty("fname");

        ComponentContext componentContext = mock(ComponentContext.class);
        doReturn(dictionary).when(componentContext).getProperties();
        doReturn(bundleContext).when(componentContext).getBundleContext();

        assertEquals("cvalue", lookupConfigurationThenFramework(componentContext, "cname", "fname"));
        assertEquals("fvalue", lookupFrameworkThenConfiguration(componentContext, "cname", "fname"));
    }

    @Test
    public void filterBuilding() throws InvalidSyntaxException {
        StringBuilder b = new StringBuilder();

        assertEquals("foo\\\\bar\\(foo\\)bar\\*foo", appendEscapedLdapValue(b, "foo\\bar(foo)bar*foo").toString());
        b.setLength(0);

        assertEquals("(foo=bar)", appendLdapFilterAttribute(b, "foo", "bar").toString());
        b.setLength(0);

        assertEquals("(foo=\\(bar\\))", appendLdapFilterAttribute(b, "foo", "(bar)").toString());
        b.setLength(0);

        assertEquals("(!(foo=*))", appendLdapFilterAttribute(b, "foo", null).toString());
        b.setLength(0);

        Map<String, String> m = newLinkedHashMap();
        m.put("foo", "bar");
        m.put("empty", null);
        m.put("escaped", "*xyz)");
        assertEquals(FrameworkUtil.createFilter("(&(objectClass=java.lang.String)(foo=bar)(!(empty=*))(escaped=\\*xyz\\)))"), getFilter(String.class, m));
        b.setLength(0);
    }
}
