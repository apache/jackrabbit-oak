package org.apache.jackrabbit.oak.jcr.query;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.qos.logback.classic.Level;

public class QueryResultDebugIteratorTest {

    private static final String QUERY_STRING = "JCR QUERY";
    private static final String QUERY_LANGUAGE="xpath";

    private static final int QUERY_RESULT_SIZE = 11_000;

    LogCustomizer logCustomizer;

    @Before
    public void setup() {
        logCustomizer = LogCustomizer.forLogger(QueryResultDebugIterator.class)
                .enable(Level.TRACE)
                .create();
    }

    @After
    public void cleanup() {
        logCustomizer.finished();
    }

    @Test
    public void testLogging() {
        Integer[] rawQueryResult = new Integer[QUERY_RESULT_SIZE];
        for (int i=0; i < QUERY_RESULT_SIZE; i++) {
            rawQueryResult[i] = i;
        }
        Iterator<Integer> rawQueryIterator = Arrays.asList(rawQueryResult).iterator();
        QueryResultDebugIterator<?> qrdi = new QueryResultDebugIterator<Integer>(rawQueryIterator,QUERY_LANGUAGE,QUERY_STRING);

        logCustomizer.starting();
        // read all results
        for (int i=0; i < QUERY_RESULT_SIZE; i++) {
            assertEquals(i, qrdi.next());
        }
        List<String> logs =  logCustomizer.getLogs();
        assertEquals(3, logs.size()); // for 1_000, 10_000 and 11_000
    }
}
