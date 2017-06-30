package org.sagebionetworks.workers.util.semaphore;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressingRunner;
import org.sagebionetworks.database.semaphore.CountingSemaphore;
import org.sagebionetworks.database.semaphore.LockReleaseFailedException;

public class SemaphoreGatedRunnerImplTest {

	@Mock
	CountingSemaphore mockSemaphore;
	SemaphoreGatedRunnerConfiguration config;
	SemaphoreGatedRunnerImpl gate;
	@Mock
	ProgressingRunner mockRunner;
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
		gate = new SemaphoreGatedRunnerImpl(mockSemaphore, config);
		
		atoken = "atoken";
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(atoken);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testConfigureBad(){
		mockRunner = null;
		config = new SemaphoreGatedRunnerConfiguration(mockRunner, lockKey, lockTimeoutSec, maxLockCount);
		gate = new SemaphoreGatedRunnerImpl(mockSemaphore, config);
	}
	
	@Test
	public void testHappy() throws Exception{
		// start the gate
		gate.run();
		// runner should be run
		verify(mockRunner).run(any(ProgressCallback.class));
		// The lock should get released.
		verify(mockSemaphore).releaseLock(lockKey, atoken);
		// The lock should not be refreshed for this case.
		verify(mockSemaphore, never()).refreshLockTimeout(anyString(), anyString(), anyLong());
	}
	
	@Test
	public void testLockReleaseOnException() throws Exception{
		// The lock must be released on exception.
		// Simulate an exception thrown by the runner.
		doThrow(new RuntimeException("Something went wrong!")).when(mockRunner).run(any(ProgressCallback.class));
		// Issue a lock.
		String atoken = "atoken";
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(atoken);
		gate.run();
		// The lock should get released.
		verify(mockSemaphore).releaseLock(lockKey, atoken);
	}
	
	@Test
	public void testLockNotAcquired() throws Exception{
		// Null is returned when a lock cannot be acquired.
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(null);
		// Start the run
		gate.run();
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
		gate.run();
		// the lock should not be released or refreshed.
		verify(mockSemaphore, never()).refreshLockTimeout(anyString(), anyString(), anyLong());
		verify(mockSemaphore, never()).releaseLock(anyString(), anyString());
		// The worker should not get called.
		verify(mockRunner, never()).run(any(ProgressCallback.class));
	}
	
//	@Test
//	public void testProgress() throws Exception{		
//		// Setup the runner to make progress at twice
//		doAnswer(new Answer<Void>() {
//
//			public Void answer(InvocationOnMock invocation) throws Throwable {
//				ProgressCallback callback = (ProgressCallback) invocation.getArguments()[0];
//				// once
//				callback.progressMade(null);
//				// twice
//				callback.progressMade(null);
//				return null;
//			}
//		}).when(mockRunner).run(any(ProgressCallback.class));
//		// start the gate
//		gate.run();
//		// The lock should get refreshed once due to throttling.
//		verify(mockSemaphore, times(1)).refreshLockTimeout(lockKey, atoken, lockTimeoutSec);
//		// The lock should get released.
//		verify(mockSemaphore).releaseLock(lockKey, atoken);
//	}
	
	@Test (expected=LockReleaseFailedException.class)
	public void testLockReleaseFailures() throws Exception{
		String atoken = "atoken";
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(atoken);
		doThrow(new LockReleaseFailedException("Failed to release the lock!")).when(mockSemaphore).releaseLock(lockKey,  atoken);
		// start the gate
		gate.run();
	}
	
	@Test
	public void testUseProgressHeartbeatDisabled() throws Exception{
		// disable the heartbeat.
		config.setUseProgressHeartbeat(false);
		gate = new SemaphoreGatedRunnerImpl(mockSemaphore, config);
		
		// Setup the worker to sleep without making progress.
		doAnswer(new Answer<Void>() {

			public Void answer(InvocationOnMock invocation) throws Throwable {
				Thread.sleep(lockTimeoutMS*2);
				return null;
			}
		}).when(mockRunner).run(any(ProgressCallback.class));
		
		// call under test.
		gate.run();
		//  heartbeat progress events should not occur
		verify(mockSemaphore, never()).refreshLockTimeout(anyString(), anyString(), anyLong());
	}
	
	@Test
	public void testUseProgressHeartbeatEnabled() throws Exception{
		// enable the heartbeat.
		config.setUseProgressHeartbeat(true);
		gate = new SemaphoreGatedRunnerImpl(mockSemaphore, config);
		
		// Setup the worker to sleep without making progress.
		doAnswer(new Answer<Void>() {

			public Void answer(InvocationOnMock invocation) throws Throwable {
				// wait for the heartbeat to fire
				Thread.sleep(lockTimeoutMS);
				return null;
			}
		}).when(mockRunner).run(any(ProgressCallback.class));
		
		// call under test.
		gate.run();
		// heartbeat progress events should occur
		verify(mockSemaphore, atLeast(2)).refreshLockTimeout(anyString(), anyString(), anyLong());
	}
}
