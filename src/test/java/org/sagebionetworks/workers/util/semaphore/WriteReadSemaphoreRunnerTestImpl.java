package org.sagebionetworks.workers.util.semaphore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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
	String precursorToken;
	String writeToken;

	
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
		
		precursorToken = "aPrecursorToken";
		when(mockWriteReadSemaphore.acquireWriteLockPrecursor(lockKey, lockTimeoutSec)).thenReturn(precursorToken);
		writeToken = "aWriteToken";
		// Return null twice and the lock on the third try.
		when(mockWriteReadSemaphore.acquireWriteLock(lockKey, precursorToken, lockTimeoutSec)).thenReturn(null,  null, writeToken);
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
	
	@Test
	public void testReadCallalbeException() throws Exception{
		Exception error = new RuntimeException("Failed");
		doThrow(error).when(mockCallable).call(any(ProgressCallback.class));
		// the passed callback can be null
		mockProgressCallback = null;
		String readToken = "aReadToken";
		when(mockWriteReadSemaphore.acquireReadLock(lockKey, lockTimeoutSec)).thenReturn(readToken);
		// call under test.
		String results;
		try {
			results = runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
			fail("Should have failed");
		} catch (Exception e) {
			// expected
		}
		// lock should be release even for an error
		verify(mockWriteReadSemaphore).releaseReadLock(lockKey, readToken);
	}
	
	@Test
	public void testTryRunWithWriteLockHappy() throws Exception{
		// call under test.
		String results = runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
		assertEquals(defaultResults, results);
		verify(mockWriteReadSemaphore).releaseWriteLock(lockKey, writeToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockWriteReadSemaphore, times(3)).refreshWriteLock(lockKey, writeToken, lockTimeoutSec);
		// progress must be forwarded to the provided callback.
		verify(mockProgressCallback).progressMade("one");
		verify(mockProgressCallback).progressMade("two");
		verify(mockProgressCallback).progressMade("three");
	}
	
	@Test
	public void testTryRunWithWriteLockNullCallback() throws Exception{
		mockProgressCallback = null;
		// call under test.
		String results = runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
		assertEquals(defaultResults, results);
		verify(mockWriteReadSemaphore).releaseWriteLock(lockKey, writeToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockWriteReadSemaphore, times(3)).refreshWriteLock(lockKey, writeToken, lockTimeoutSec);
	}
	
	@Test (expected=LockUnavilableException.class)
	public void testTryRunWithWriteLockUnavailable() throws Exception{
		// null token should result in 
		String precursorToken = null;
		when(mockWriteReadSemaphore.acquireWriteLockPrecursor(lockKey, lockTimeoutSec)).thenReturn(precursorToken);
		String writeToken = "aWriteToken";
		// Return null twice and the lock on the third try.
		when(mockWriteReadSemaphore.acquireWriteLock(lockKey, precursorToken, lockTimeoutSec)).thenReturn(null,  null, writeToken);
		// call under test.
		runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
	}

	@Test (expected=IllegalArgumentException.class)
	public void testTryRunWithWriteLockNullKey() throws Exception{
		lockKey = null;
		// call under test.
		runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testTryRunWithWriteLockSmallTimeout() throws Exception{
		lockTimeoutSec = 1;
		// call under test.
		runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testTryRunWithWriteLockNullCallable() throws Exception{
		mockCallable = null;
		// call under test.
		runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test
	public void testTryRunWithWriteException() throws Exception{
		Exception error = new RuntimeException("An error");
		// call under test.
		try {
			runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
		} catch (Exception e) {
			assertEquals(error, e);
		}
		// the write lock should still be released.
		verify(mockWriteReadSemaphore).releaseWriteLock(lockKey, writeToken);
	}
}
