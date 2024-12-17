package org.apache.jackrabbit.oak.commons.jdkcompat;

import org.junit.Test;

import javax.security.auth.Subject;

import java.security.PrivilegedAction;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class Java23SubjectTest {

    static int specVersion = Runtime.version().feature();
    
    @Test
    public void testApiExistence() {
        if (specVersion > 17) {
            assertNotNull(Java23Subject.current);
            assertNotNull(Java23Subject.callAs);
        } else {
            assertNull(Java23Subject.current);
            assertNull(Java23Subject.callAs);
        }
    }

    @Test
    public void testApiFunction() {
        Subject subject = new Subject();
        if (specVersion > 17) {
            assertEquals(subject,
                    Java23Subject.doAs(subject, (PrivilegedAction<Subject>) () -> {
                        assertEquals(Java23Subject.getSubject(), subject);
                        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                        assertFalse(Arrays.stream(stackTrace)
                                .map(elt -> elt.getMethodName())
                                .filter(name -> "callAs".equals(name))
                                .findFirst()
                                .isEmpty());
                        return subject;
                    }));
        } else {
            assertEquals(subject,
                    Java23Subject.doAs(subject, (PrivilegedAction<Subject>) () -> {
                        assertEquals(Java23Subject.getSubject(), subject);
                        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                        assertTrue(Arrays.stream(stackTrace)
                                .map(elt -> elt.getMethodName())
                                .filter(name -> "doAs".equals(name)).count() == 2);
                        return subject;
                    }));
        }
    }
}
