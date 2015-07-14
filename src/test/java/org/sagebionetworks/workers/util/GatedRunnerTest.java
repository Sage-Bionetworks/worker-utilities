package org.sagebionetworks.workers.util;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class GatedRunnerTest {
	
	Gate mockGate;
	Runnable mockRunner;
	GatedRunner gatedRunner;
	
	@Before
	public void before(){
		mockRunner = Mockito.mock(Runnable.class);
		mockGate = Mockito.mock(Gate.class);
		gatedRunner = new GatedRunner(mockGate, mockRunner);
	}
	
	@Test
	public void testCanRunTrue(){
		when(mockGate.canRun()).thenReturn(true);
		gatedRunner.run();
		verify(mockRunner).run();
	}

	@Test
	public void testCanRunFalse(){
		when(mockGate.canRun()).thenReturn(false);
		gatedRunner.run();
		verify(mockRunner, never()).run();
	}
	
	@Test
	public void testFailure(){
		when(mockGate.canRun()).thenReturn(true);
		Exception error = new RuntimeException("Something went wrong");
		doThrow(error).when(mockRunner).run();
		// call under test
		gatedRunner.run();
		verify(mockRunner).run();
		verify(mockGate).runFailed(error);
	}
}
