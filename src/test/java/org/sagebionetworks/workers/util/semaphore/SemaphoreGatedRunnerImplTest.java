package org.sagebionetworks.workers.util.semaphore;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressingRunner;
import org.sagebionetworks.database.semaphore.CountingSemaphore;
import org.sagebionetworks.database.semaphore.LockReleaseFailedException;
import org.sagebionetworks.workers.util.Gate;

public class SemaphoreGatedRunnerImplTest {

	@Mock
	CountingSemaphore mockSemaphore;
	SemaphoreGatedRunnerConfiguration config;
	SemaphoreGatedRunnerImpl semaphoreGatedRunner;
	@Mock
	ProgressingRunner mockRunner;

	@Mock
	Gate mockGate;
	String lockKey;
	long lockTimeoutSec;
	long lockTimeoutMS;
	int maxLockCount;
	
	String atoken;
	
	@Before
	public void before(){
		MockitoAnnotations.initMocks(this);
		
		lockKey = "aKey";
		lockTimeoutSec = 4;
		lockTimeoutMS = lockTimeoutSec*1000;
		maxLockCount = 2;

		config = new SemaphoreGatedRunnerConfiguration(mockRunner, lockKey, lockTimeoutSec, maxLockCount);
		semaphoreGatedRunner = new SemaphoreGatedRunnerImpl(mockSemaphore, config, mockGate);
		
		atoken = "atoken";
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(atoken);
		when(mockGate.canRun()).thenReturn(true);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testConfigureBad(){
		mockRunner = null;
		config = null;
		semaphoreGatedRunner = new SemaphoreGatedRunnerImpl(mockSemaphore, config, mockGate);
	}
	
	@Test
	public void testHappy() throws Exception{
		// start the semaphoreGatedRunner
		semaphoreGatedRunner.run();
		// runner should be run
		verify(mockRunner).run(any(ProgressCallback.class));
		// The lock should get released.
		verify(mockSemaphore).releaseLock(lockKey, atoken);
		// The lock should be refreshed for this case.
		verify(mockSemaphore).refreshLockTimeout(anyString(), anyString(), anyLong());
	}
	
	@Test
	public void testLockReleaseOnException() throws Exception{
		// The lock must be released on exception.
		// Simulate an exception thrown by the runner.
		doThrow(new RuntimeException("Something went wrong!")).when(mockRunner).run(any(ProgressCallback.class));
		// Issue a lock.
		String atoken = "atoken";
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(atoken);
		semaphoreGatedRunner.run();
		// The lock should get released.
		verify(mockSemaphore).releaseLock(lockKey, atoken);
	}
	
	@Test
	public void testLockNotAcquired() throws Exception{
		// Null is returned when a lock cannot be acquired.
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(null);
		// Start the run
		semaphoreGatedRunner.run();
		// the lock should not be released or refreshed.
		verify(mockSemaphore, never()).refreshLockTimeout(anyString(), anyString(), anyLong());
		verify(mockSemaphore, never()).releaseLock(anyString(), anyString());
		// The worker should not get called.
		verify(mockRunner, never()).run(any(ProgressCallback.class));
	}
	
	@Test
	public void testExceptionOnAcquireLock() throws Exception{
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenThrow(new OutOfMemoryError("Something bad!"));
		// Start the run. The exception should not make it out of the runner.
		semaphoreGatedRunner.run();
		// the lock should not be released or refreshed.
		verify(mockSemaphore, never()).refreshLockTimeout(anyString(), anyString(), anyLong());
		verify(mockSemaphore, never()).releaseLock(anyString(), anyString());
		// The worker should not get called.
		verify(mockRunner, never()).run(any(ProgressCallback.class));
	}
	
	@Test (expected=LockReleaseFailedException.class)
	public void testLockReleaseFailures() throws Exception{
		String atoken = "atoken";
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(atoken);
		doThrow(new LockReleaseFailedException("Failed to release the lock!")).when(mockSemaphore).releaseLock(lockKey,  atoken);
		// start the semaphoreGatedRunner
		semaphoreGatedRunner.run();
	}
	
	@Test
	public void testProgressHeartbeat() throws Exception{
		// disable the heartbeat.
		semaphoreGatedRunner = new SemaphoreGatedRunnerImpl(mockSemaphore, config, mockGate);
		setupRunnerSleep();

		// call under test.
		semaphoreGatedRunner.run();

		//  heartbeat progress events should occur
		verify(mockSemaphore, atLeast(2)).refreshLockTimeout(anyString(), anyString(), anyLong());
	}

	@Test
	public void testRun_canRunIsTrue() throws Exception {
		when(mockGate.canRun()).thenReturn(true);
		setupRunnerSleep();

		// call under test.
		semaphoreGatedRunner.run();

		verify(mockRunner).run(any(ProgressCallback.class));
		verify(mockSemaphore, atLeast(2)).refreshLockTimeout(anyString(), anyString(), anyLong());
	}

	@Test
	public void testRun_gateIsNull() throws Exception {
		semaphoreGatedRunner = new SemaphoreGatedRunnerImpl(mockSemaphore, config, null);
		setupRunnerSleep();

		// call under test.
		semaphoreGatedRunner.run();

		verify(mockRunner).run(any(ProgressCallback.class));
		verify(mockSemaphore, atLeast(2)).refreshLockTimeout(anyString(), anyString(), anyLong());
	}

	@Test
	public void testRun_canRunIsFalse(){
		when(mockGate.canRun()).thenReturn(false);

		// call under test.
		semaphoreGatedRunner.run();

		verifyZeroInteractions(mockRunner);
	}

	@Test
	public void testRunWithCanRunTrueAndThenFalse() throws Exception {
		when(mockGate.canRun()).thenReturn(true, false);
		setupRunnerSleep();

		// call under test.
		semaphoreGatedRunner.run();

		verify(mockRunner).run(any(ProgressCallback.class));
		// Fix for PLFM-7432: refresh should occur even when canRun() is false.
		verify(mockSemaphore, atLeast(2)).refreshLockTimeout(anyString(), anyString(), anyLong());
	}


	@Test
	public void canRun_nullGate(){
		semaphoreGatedRunner = new SemaphoreGatedRunnerImpl(mockSemaphore, config, null);
		assertTrue(semaphoreGatedRunner.canRun());
	}

	@Test
	public void canRun_GateCanRunTrue(){
		assertTrue(semaphoreGatedRunner.canRun());
	}

	@Test
	public void canRun_GateCanRunFalse(){
		when(mockGate.canRun()).thenReturn(false);
		assertFalse(semaphoreGatedRunner.canRun());
	}


	private void setupRunnerSleep() throws Exception {
		// Setup the worker to sleep without making progress.
		doAnswer(invocation -> {
			Thread.sleep(lockTimeoutMS*2);
			return null;
		}).when(mockRunner).run(any(ProgressCallback.class));
	}
}
