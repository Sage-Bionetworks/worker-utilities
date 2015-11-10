package org.sagebionetworks.workers.util.semaphore;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.sagebionetworks.database.semaphore.WriteReadSemaphore;
import org.sagebionetworks.workers.util.Clock;
import org.sagebionetworks.workers.util.progress.ProgressCallback;
import org.sagebionetworks.workers.util.progress.ProgressingCallable;

public class WriteReadSemaphoreRunnerTestImpl {
	
	WriteReadSemaphore mockWriteReadSemaphore;
	Clock mockClock;
	ProgressCallback<String> mockProgressCallback;
	ProgressingCallable<String> mockCallable;
	WriteReadSemaphoreRunner runner;
	String lockKey;
	long lockTimeoutSec;
	String defaultResults;
	
	@Before
	public void before() throws Exception{
		mockWriteReadSemaphore = Mockito.mock(WriteReadSemaphore.class);
		mockClock = Mockito.mock(Clock.class);
		mockProgressCallback = Mockito.mock(ProgressCallback.class);
		mockCallable = Mockito.mock(ProgressingCallable.class);
		// simulate 10 seconds between calls.
		when(mockClock.currentTimeMillis()).thenReturn(0L,1000*10L,1000*20L,1000*30L);
		lockKey = "123";
		lockTimeoutSec = 10;
		runner = new WriteReadSemaphoreRunnerImpl(mockWriteReadSemaphore, mockClock);
		
		defaultResults = "someResults";
		// Setup the callable to call progressMade()
		doAnswer(new Answer<String>(){

			@Override
			public String answer(InvocationOnMock invocation) throws Throwable {
				ProgressCallback<String> callback = (ProgressCallback<String>) invocation.getArguments()[0];
				// make some progress
				callback.progressMade("one");
				callback.progressMade("two");
				callback.progressMade("three");
				return defaultResults;
			}}).when(mockCallable).call(any(ProgressCallback.class));
	}
	
	@Test
	public void testReadCallalbeHappy() throws Exception{
		String readToken = "aReadToken";
		when(mockWriteReadSemaphore.acquireReadLock(lockKey, lockTimeoutSec)).thenReturn(readToken);
		// call under test.
		String results = runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
		assertEquals(defaultResults, results);
		verify(mockWriteReadSemaphore).releaseReadLock(lockKey, readToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockWriteReadSemaphore, times(3)).refreshReadLock(lockKey, readToken, lockTimeoutSec);
		// progress must be forwarded to the provided callback.
		verify(mockProgressCallback).progressMade("one");
		verify(mockProgressCallback).progressMade("two");
		verify(mockProgressCallback).progressMade("three");
	}
	
	@Test
	public void testReadCallalbeNullCallback() throws Exception{
		// the passed callback can be null
		mockProgressCallback = null;
		String readToken = "aReadToken";
		when(mockWriteReadSemaphore.acquireReadLock(lockKey, lockTimeoutSec)).thenReturn(readToken);
		// call under test.
		String results = runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
		assertEquals(defaultResults, results);
		verify(mockWriteReadSemaphore).releaseReadLock(lockKey, readToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockWriteReadSemaphore, times(3)).refreshReadLock(lockKey, readToken, lockTimeoutSec);
	}
	
	@Test (expected=LockUnavilableException.class)
	public void testReadCallalbeLockUnavaiable() throws Exception{
		// null token means the lock cannot be acquired.
		when(mockWriteReadSemaphore.acquireReadLock(lockKey, lockTimeoutSec)).thenReturn(null);
		// call under test.
		runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testReadCallalbeNullKey() throws Exception{
		lockKey = null;
		// call under test.
		runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testReadCallalbeNullCallable() throws Exception{
		mockCallable = null;
		// call under test.
		runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testReadCallalbeSmallTimeout() throws Exception{
		lockTimeoutSec = 1;
		// call under test.
		runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
	}

}
