package org.sagebionetworks.workers.util.semaphore;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressingRunner;
import org.sagebionetworks.database.semaphore.CountingSemaphore;
import org.sagebionetworks.workers.util.Gate;

public class SemaphoreGatedWorkerStackTest {
	
	@Mock
	CountingSemaphore mockSemaphore;
	@Mock
	ProgressingRunner mockRunner;
	@Mock
	Gate mockGate;
	SemaphoreGatedWorkerStackConfiguration config;

	@Before
	public void before(){
		MockitoAnnotations.initMocks(this);
		
		// mock semaphore
		String token = "aToken";
		when(mockSemaphore.attemptToAcquireLock(any(String.class),
						anyLong(), anyInt())).thenReturn(token);
		
		// mock semaphoreGatedRunner
		when(mockGate.canRun()).thenReturn(true);
		
		config = new SemaphoreGatedWorkerStackConfiguration();
		config.setGate(mockGate);
		config.setProgressingRunner(mockRunner);
		config.setSemaphoreLockKey("lockKey");
		config.setSemaphoreLockTimeoutSec(10);
		config.setSemaphoreMaxLockCount(2);
	}
	
	@Test
	public void testHappyRun() throws Exception{
		SemaphoreGatedWorkerStack stack = new SemaphoreGatedWorkerStack(mockSemaphore, config);
		// call under test
		stack.run();
		verify(mockRunner).run(any(ProgressCallback.class));
	}

	@Test
	public void testGateCanRunFalse() throws Exception{
		when(mockGate.canRun()).thenReturn(false);
		SemaphoreGatedWorkerStack stack = new SemaphoreGatedWorkerStack(mockSemaphore, config);
		// call under test
		stack.run();
		verify(mockRunner, never()).run(any(ProgressCallback.class));
	}
	
	@Test
	public void testRunNoSemaphoreLock() throws Exception{
		when(mockSemaphore.attemptToAcquireLock(any(String.class),
				anyLong(), anyInt())).thenReturn(null);
		SemaphoreGatedWorkerStack stack = new SemaphoreGatedWorkerStack(mockSemaphore, config);
		// call under test
		stack.run();
		verify(mockRunner, never()).run(any(ProgressCallback.class));
	}
	
	@Test
	public void testNullGateHappy() throws Exception{
		config.setGate(null);
		SemaphoreGatedWorkerStack stack = new SemaphoreGatedWorkerStack(mockSemaphore, config);
		// call under test
		stack.run();
		verify(mockRunner).run(any(ProgressCallback.class));
	}
	
	@Test
	public void testNullGateRunNoSemaphoreLock() throws Exception{
		config.setGate(null);
		when(mockSemaphore.attemptToAcquireLock(any(String.class),
				anyLong(), anyInt())).thenReturn(null);
		SemaphoreGatedWorkerStack stack = new SemaphoreGatedWorkerStack(mockSemaphore, config);
		// call under test
		stack.run();
		verify(mockRunner, never()).run(any(ProgressCallback.class));
	}
}
