package org.apache.jackrabbit.cluster.test.insistent.impl;

import org.apache.jackrabbit.cluster.test.insistent.InsistentChangePack;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.jcr.InvalidItemStateException;
import javax.jcr.Session;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 01/03/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class InsistentSessionImplTest {

    @Mock
    Session sessionMock;

    @Mock
    InsistentChangePack insistentChangePackMock;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private InsistentSessionImpl underTest;

    @Before
    public void setUp() {
        underTest = new InsistentSessionImpl(sessionMock);
    }

    @Test
    public void shouldConstructWithJcrSession() {
        assertThat(underTest.getSession(), is(sessionMock));
    }

    @Test
    public void shouldNotCallSaveDirectly() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Usage of safe() not permitted. Please use save(InsistentChangePack insistentChangPack) instead");
        underTest.save();
    }

    @Test
    public void shouldSaveOnFirstAttempt() throws Exception {
        underTest.save(insistentChangePackMock);
        verify(insistentChangePackMock).write();
    }

    @Test
    public void shouldSaveOnSecondAttempt() throws Exception {
        // given
        doThrow(InvalidItemStateException.class).doNothing().when(sessionMock).save();

        // when
        underTest.save(insistentChangePackMock);

        // then
        verify(insistentChangePackMock, times(2)).write();
    }

    @Test
    public void shouldSaveOnThirdAttempt() throws Exception {
        // given
        doThrow(InvalidItemStateException.class)
                .doThrow(InvalidItemStateException.class)
                .doNothing()
                .when(sessionMock).save();

        // when
        underTest.save(insistentChangePackMock);

        // then
        verify(insistentChangePackMock, times(3)).write();
    }

    @Test
    public void shouldThrowExceptionAfterOngoingErrors() throws Exception {
        // given
        final String exceptionMessage = "exception message";
        doThrow(new InvalidItemStateException(exceptionMessage))
                .when(sessionMock).save();

        // then / when
        expectedException.expect(InvalidItemStateException.class);
        expectedException.expectMessage(exceptionMessage);
        underTest.save(insistentChangePackMock);
    }


}