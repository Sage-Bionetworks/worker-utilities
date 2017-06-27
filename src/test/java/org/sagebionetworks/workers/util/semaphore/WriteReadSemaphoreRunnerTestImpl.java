package org.sagebionetworks.workers.util.semaphore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.sagebionetworks.common.util.Clock;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressListener;
import org.sagebionetworks.common.util.progress.ProgressingCallable;
import org.sagebionetworks.common.util.progress.SimpleProgressCallback;
import org.sagebionetworks.database.semaphore.WriteReadSemaphore;

public class WriteReadSemaphoreRunnerTestImpl {
	
	@Mock
	WriteReadSemaphore mockWriteReadSemaphore;
	@Mock
	Clock mockClock;
	ProgressCallback<String> progressCallback;
	@Mock
	ProgressCallback<String> mockProgressCallback;
	@Mock
	ProgressingCallable<Integer, String> mockCallable;
	WriteReadSemaphoreRunner runner;
	String lockKey;
	int lockTimeoutSec;
	Integer defaultResults;
	String precursorToken;
	String writeToken;
	String readToken;
	List<ProgressListener<Void>> progressListeners;

	
	@Before
	public void before() throws Exception{
		MockitoAnnotations.initMocks(this);
		
		progressCallback = new SimpleProgressCallback<String>();
		// simulate 10 seconds between calls.
		when(mockClock.currentTimeMillis()).thenReturn(0L,1000*10L,1000*20L,1000*30L);
		lockKey = "123";
		lockTimeoutSec = 10;
		runner = new WriteReadSemaphoreRunnerImpl(mockWriteReadSemaphore, mockClock);
		
		defaultResults = 999;
		// Setup the callable to call progressMade()
		doAnswer(new Answer<Integer>(){

			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable {
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
		
		readToken = "aReadToken";
		when(mockWriteReadSemaphore.acquireReadLock(lockKey, lockTimeoutSec)).thenReturn(readToken);
	}
	
	@Test
	public void testReadCallalbeHappy() throws Exception{
		// call under test.
		Integer results = runner.tryRunWithReadLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
		assertEquals(defaultResults, results);
		verify(mockWriteReadSemaphore).releaseReadLock(lockKey, readToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockWriteReadSemaphore, times(3)).refreshReadLock(lockKey, readToken, lockTimeoutSec);
	}
	
	@Test
	public void testReadCallalbeRemoveListner() throws Exception{
		// call under test.
		runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
		// verify that the listener is added and removed.
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testReadCallalbeNullCallback() throws Exception{
		// the passed callback can be null
		progressCallback = null;
		String readToken = "aReadToken";
		when(mockWriteReadSemaphore.acquireReadLock(lockKey, lockTimeoutSec)).thenReturn(readToken);
		// call under test.
		Integer results = runner.tryRunWithReadLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=LockUnavilableException.class)
	public void testReadCallalbeLockUnavaiable() throws Exception{
		// null token means the lock cannot be acquired.
		when(mockWriteReadSemaphore.acquireReadLock(lockKey, lockTimeoutSec)).thenReturn(null);
		// call under test.
		runner.tryRunWithReadLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testReadCallalbeNullKey() throws Exception{
		lockKey = null;
		// call under test.
		runner.tryRunWithReadLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testReadCallalbeNullCallable() throws Exception{
		mockCallable = null;
		// call under test.
		runner.tryRunWithReadLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testReadCallalbeSmallTimeout() throws Exception{
		lockTimeoutSec = WriteReadSemaphoreRunnerImpl.MINIMUM_LOCK_TIMEOUT_SEC-1;
		// call under test.
		runner.tryRunWithReadLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	
	@Test
	public void testReadCallalbeRemoveListenerException() throws Exception{
		Exception error = new RuntimeException("Failed");
		doThrow(error).when(mockCallable).call(any(ProgressCallback.class));
		// call under test.
		try {
			runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
			fail("Should have failed");
		} catch (Exception e) {
			// expected
		}
		// lock should be release even for an error
		verify(mockWriteReadSemaphore).releaseReadLock(lockKey, readToken);
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}
	
	@Test
	public void testTryRunWithWriteLockHappy() throws Exception{
		// call under test.
		Integer results = runner.tryRunWithWriteLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
		assertEquals(defaultResults, results);
		verify(mockWriteReadSemaphore).releaseWriteLock(lockKey, writeToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockWriteReadSemaphore, times(3)).refreshWriteLock(lockKey, writeToken, lockTimeoutSec);
	}
	
	@Test
	public void testWriteCallalbeRemoveListner() throws Exception{
		// call under test.
		runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
		// verify that the listener is added and removed.
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testTryRunWithWriteLockNullCallback() throws Exception{
		progressCallback = null;
		// call under test.
		Integer results = runner.tryRunWithWriteLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
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
		runner.tryRunWithWriteLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
	}

	@Test (expected=IllegalArgumentException.class)
	public void testTryRunWithWriteLockNullKey() throws Exception{
		lockKey = null;
		// call under test.
		runner.tryRunWithWriteLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testTryRunWithWriteLockSmallTimeout() throws Exception{
		lockTimeoutSec = WriteReadSemaphoreRunnerImpl.MINIMUM_LOCK_TIMEOUT_SEC-1;
		// call under test.
		runner.tryRunWithWriteLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testTryRunWithWriteLockNullCallable() throws Exception{
		mockCallable = null;
		// call under test.
		runner.tryRunWithWriteLock(progressCallback, lockKey, lockTimeoutSec, mockCallable);
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
		// verify that the listener is added and removed even with an exception.
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}
}
