package org.apache.jackrabbit.oak.spi.security.authentication;

import org.junit.Test;
import org.mockito.Answers;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.withSettings;

public class LoginModuleMonitorTest {

    private final LoginModuleMonitor noop = spy(LoginModuleMonitor.NOOP);
    private final LoginModuleMonitor monitor = mock(LoginModuleMonitor.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));

    @Test
    public void testLoginError() {
        noop.loginError();
        verifyNoInteractions(monitor);
        reset(noop, monitor);

        monitor.loginError();
        verifyNoInteractions(noop);
    }

    @Test
    public void testGetMonitorClass() {
        assertSame(LoginModuleMonitor.class, noop.getMonitorClass());
        assertSame(LoginModuleMonitor.class, monitor.getMonitorClass());
    }

    @Test
    public void testGetMonitorProperties() {
        assertTrue(noop.getMonitorProperties().isEmpty());
        assertTrue(monitor.getMonitorProperties().isEmpty());
    }
}
