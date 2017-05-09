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

package org.apache.jackrabbit.oak.remote.content;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.remote.RemoteValue;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class ContentRemoteResultTest {

    private ContentRemoteResult createResult(ResultRow row) {
        return new ContentRemoteResult(mock(ContentRemoteBinaries.class), row);
    }

    private ContentRemoteResult createResult(ContentRemoteBinaries binaries, ResultRow row) {
        return new ContentRemoteResult(binaries, row);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testColumnNotAvailable() {
        ResultRow row = mock(ResultRow.class);
        doThrow(IllegalArgumentException.class).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        result.getColumnValue("column");
    }

    @Test
    public void testStringColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.STRING).when(value).getType();
        doReturn("value").when(value).getValue(Type.STRING);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals("value", remoteValue.asText());
    }

    @Test
    public void testBinaryColumn() {
        Blob blob = mock(Blob.class);

        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.BINARY).when(value).getType();
        doReturn(blob).when(value).getValue(Type.BINARY);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteBinaries binaries = mock(ContentRemoteBinaries.class);
        doReturn("id").when(binaries).put(blob);

        ContentRemoteResult result = createResult(binaries, row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals("id", remoteValue.asBinaryId());
    }

    @Test
    public void testLongColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.LONG).when(value).getType();
        doReturn(42L).when(value).getValue(Type.LONG);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(42L, remoteValue.asLong().longValue());
    }

    @Test
    public void testDoubleColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.DOUBLE).when(value).getType();
        doReturn(4.2).when(value).getValue(Type.DOUBLE);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(4.2, remoteValue.asDouble().doubleValue(), 1e-5);
    }

    @Test
    public void testDateColumn() {
        Date now = new Date();

        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.DATE).when(value).getType();
        doReturn(toFormattedDate(now)).when(value).getValue(Type.DATE);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(now.getTime(), remoteValue.asDate().longValue());
    }

    @Test
    public void testBooleanColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.BOOLEAN).when(value).getType();
        doReturn(true).when(value).getValue(Type.BOOLEAN);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(true, remoteValue.asBoolean());
    }

    @Test
    public void testNameColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.NAME).when(value).getType();
        doReturn("value").when(value).getValue(Type.NAME);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals("value", remoteValue.asName());
    }

    @Test
    public void testPathColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.PATH).when(value).getType();
        doReturn("value").when(value).getValue(Type.PATH);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals("value", remoteValue.asPath());
    }

    @Test
    public void testReferenceColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.REFERENCE).when(value).getType();
        doReturn("value").when(value).getValue(Type.REFERENCE);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals("value", remoteValue.asReference());
    }

    @Test
    public void testWeakReferenceColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.WEAKREFERENCE).when(value).getType();
        doReturn("value").when(value).getValue(Type.WEAKREFERENCE);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals("value", remoteValue.asWeakReference());
    }

    @Test
    public void testUriColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.URI).when(value).getType();
        doReturn("value").when(value).getValue(Type.URI);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals("value", remoteValue.asUri());
    }

    @Test
    public void testDecimalColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.DECIMAL).when(value).getType();
        doReturn(BigDecimal.ONE).when(value).getValue(Type.DECIMAL);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(BigDecimal.ONE, remoteValue.asDecimal());
    }

    @Test
    public void testMultiStringColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.STRINGS).when(value).getType();
        doReturn(asList("a", "b")).when(value).getValue(Type.STRINGS);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList("a", "b"), remoteValue.asMultiText());
    }

    @Test
    public void testMultiBinaryColumn() {
        Blob first = mock(Blob.class);
        Blob second = mock(Blob.class);

        ContentRemoteBinaries binaries = mock(ContentRemoteBinaries.class);
        doReturn("first").when(binaries).put(first);
        doReturn("second").when(binaries).put(second);

        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.BINARIES).when(value).getType();
        doReturn(asList(first, second)).when(value).getValue(Type.BINARIES);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(binaries, row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList("first", "second"), remoteValue.asMultiBinaryId());
    }

    @Test
    public void testMultiLongColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.LONGS).when(value).getType();
        doReturn(asList(4L, 2L)).when(value).getValue(Type.LONGS);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList(4L, 2L), remoteValue.asMultiLong());
    }

    @Test
    public void testMultiDoubleColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.DOUBLES).when(value).getType();
        doReturn(asList(4.0, 2.0)).when(value).getValue(Type.DOUBLES);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList(4.0, 2.0), remoteValue.asMultiDouble());
    }

    @Test
    public void testMultiDateColumn() {
        Date first = new Date(4);
        Date second = new Date(2);

        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.DATES).when(value).getType();
        doReturn(toFormattedDates(first, second)).when(value).getValue(Type.DATES);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList(4L, 2L), remoteValue.asMultiDate());
    }

    @Test
    public void testMultiBooleanColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.BOOLEANS).when(value).getType();
        doReturn(asList(true, false)).when(value).getValue(Type.BOOLEANS);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList(true, false), remoteValue.asMultiBoolean());
    }

    @Test
    public void testMultiNameColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.NAMES).when(value).getType();
        doReturn(asList("a", "b")).when(value).getValue(Type.NAMES);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList("a", "b"), remoteValue.asMultiName());
    }

    @Test
    public void testMultiPathColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.PATHS).when(value).getType();
        doReturn(asList("a", "b")).when(value).getValue(Type.PATHS);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList("a", "b"), remoteValue.asMultiPath());
    }

    @Test
    public void testMultiReferenceColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.REFERENCES).when(value).getType();
        doReturn(asList("a", "b")).when(value).getValue(Type.REFERENCES);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList("a", "b"), remoteValue.asMultiReference());
    }

    @Test
    public void testMultiWeakReferenceColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.WEAKREFERENCES).when(value).getType();
        doReturn(asList("a", "b")).when(value).getValue(Type.WEAKREFERENCES);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList("a", "b"), remoteValue.asMultiWeakReference());
    }

    @Test
    public void testMultiUriColumn() {
        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.URIS).when(value).getType();
        doReturn(asList("a", "b")).when(value).getValue(Type.URIS);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList("a", "b"), remoteValue.asMultiUri());
    }

    @Test
    public void testMultiDecimalColumn() {
        BigDecimal first = new BigDecimal(4);
        BigDecimal second = new BigDecimal(2);

        PropertyValue value = mock(PropertyValue.class);
        doReturn(Type.DECIMALS).when(value).getType();
        doReturn(asList(first, second)).when(value).getValue(Type.DECIMALS);

        ResultRow row = mock(ResultRow.class);
        doReturn(value).when(row).getValue("column");

        ContentRemoteResult result = createResult(row);
        RemoteValue remoteValue = result.getColumnValue("column");
        assertEquals(asList(first, second), remoteValue.asMultiDecimal());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectorNotAvailable() {
        ResultRow row = mock(ResultRow.class);
        doThrow(IllegalArgumentException.class).when(row).getPath("selector");
        createResult(row).getSelectorPath("selector");
    }

    @Test
    public void testSelector() {
        ResultRow row = mock(ResultRow.class);
        doReturn("path").when(row).getPath("selector");
        assertEquals("path", createResult(row).getSelectorPath("selector"));
    }

    private Iterable<String> toFormattedDates(Date... dates) {
        List<String> result = newArrayList();

        for (Date date : dates) {
            result.add(toFormattedDate(date));
        }

        return result;
    }

    private String toFormattedDate(Date date) {
        return ISO8601.format(toCalendar(date));
    }

    private Calendar toCalendar(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(date.getTime());
        return calendar;
    }

}
