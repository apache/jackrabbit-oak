package org.apache.jackrabbit.oak.query;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class QueryInformationLoggerTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(QueryInformationLoggerTest.class);

    private static final String EXTERNAL_LOG = QueryInformationLogger.class.getName() + ".externalQuery";
    private static final String INTERNAL_LOG = QueryInformationLogger.class.getName() + ".internalQuery";
    
    String[] ignoredClasses = new String[] {"org.apache.jackrabbit.oak","java.lang","sun.reflect", "jdk"};
    String[] allClassesIgnored = new String[] {"java","org","net","sun","jdk"};
    
    
    @Test
    public void regularQueryTest() {
        
        LogCustomizer external = LogCustomizer.forLogger(EXTERNAL_LOG).enable(Level.INFO).create();
        LogCustomizer internal = LogCustomizer.forLogger(INTERNAL_LOG).enable(Level.TRACE).create();
        try {
            external.starting();
            internal.starting();
            String query = "SELECT * FROM [cq:Page]";
            QueryInformationLogger.logCaller(query, ignoredClasses);
            assertEquals("Exactly 1 entry must be written",1,external.getLogs().size());
            String logEntry = external.getLogs().get(0);
            assertTrue(logEntry.contains("org.junit.runners.model")); // org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:59)
            assertEquals(0,internal.getLogs().size());
        } finally {
            external.finished();
            internal.finished();
        }      
    }
    
    @Test
    public void withStacktrace() {
        
        LogCustomizer external = LogCustomizer.forLogger(EXTERNAL_LOG).enable(Level.DEBUG).create();
        LogCustomizer internal = LogCustomizer.forLogger(INTERNAL_LOG).enable(Level.TRACE).create();
        try {
            external.starting();
            internal.starting();
            String query = "SELECT * FROM [cq:Page]";
            QueryInformationLogger.logCaller(query, ignoredClasses);
            assertEquals("Exactly 2 entries must be written",2,external.getLogs().size()); //     
            assertEquals(0,internal.getLogs().size());
        } finally {
            external.finished();
            internal.finished();
        }      
    }
    
    @Test
    public void testOakInternalStatement() {
        LogCustomizer external = LogCustomizer.forLogger(EXTERNAL_LOG).enable(Level.DEBUG).create();
        LogCustomizer internal = LogCustomizer.forLogger(INTERNAL_LOG).enable(Level.INFO).create();
        try {
            external.starting();
            internal.starting();
            String query = "SELECT * FROM [cq:Page] " + QueryInformationLogger.OAK_INTERNAL_MARKER;
            QueryInformationLogger.logCaller(query, ignoredClasses);
            
            // On INFO no logs are written
            assertEquals(0,external.getLogs().size()); //     
            assertEquals(0,internal.getLogs().size());
        } finally {
            external.finished();
            internal.finished();
        }     
    }
    
    @Test
    public void testWithAllStackEntriesIgnored_INFO() {
        LogCustomizer external = LogCustomizer.forLogger(EXTERNAL_LOG).enable(Level.DEBUG).create();
        LogCustomizer internal = LogCustomizer.forLogger(INTERNAL_LOG).enable(Level.INFO).create();
        try {
            external.starting();
            internal.starting();
            String query = "SELECT * FROM [cq:Page]";
            QueryInformationLogger.logCaller(query, allClassesIgnored);
            try {
                throw new AssertionError();
            } catch (AssertionError e) {
                e.printStackTrace();
            }
            
            assertEquals(0,external.getLogs().size());    
            assertEquals(0,internal.getLogs().size()); // would require DEBUG
        } finally {
            external.finished();
            internal.finished();
        }     
    }  
    
    
    @Test
    public void testWithAllStackEntriesIgnored_DEBUG() {
        LogCustomizer external = LogCustomizer.forLogger(EXTERNAL_LOG).enable(Level.DEBUG).create();
        LogCustomizer internal = LogCustomizer.forLogger(INTERNAL_LOG).enable(Level.DEBUG).create();
        try {
            external.starting();
            internal.starting();
            String query = "SELECT * FROM [cq:Page]";
            QueryInformationLogger.logCaller(query, allClassesIgnored);
            
            assertEquals(0,external.getLogs().size());    
            assertEquals(1,internal.getLogs().size()); // query statement
        } finally {
            external.finished();
            internal.finished();
        }     
    } 
    
    @Test
    public void testWithAllStackEntriesIgnored_TRACE() {
        LogCustomizer external = LogCustomizer.forLogger(EXTERNAL_LOG).enable(Level.DEBUG).create();
        LogCustomizer internal = LogCustomizer.forLogger(INTERNAL_LOG).enable(Level.TRACE).create();
        try {
            external.starting();
            internal.starting();
            String query = "SELECT * FROM [cq:Page]";
            QueryInformationLogger.logCaller(query, allClassesIgnored);
            
            // On INFO no logs are written
            assertEquals(0,external.getLogs().size());    
            assertEquals(2,internal.getLogs().size()); // query statement + Stacktrace
        } finally {
            external.finished();
            internal.finished();
        }     
    }  
    
    
}
