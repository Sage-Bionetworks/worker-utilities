package org.sagebionetworks.workers.util.semaphore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.sagebionetworks.common.util.Clock;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressListener;
import org.sagebionetworks.common.util.progress.ProgressingCallable;
import org.sagebionetworks.database.semaphore.CountingSemaphore;

@RunWith(MockitoJUnitRunner.class)
public class WriteReadSemaphoreRunnerTestImpl {
	
	@Mock
	CountingSemaphore mockCountingSemaphore;
	@Mock
	Clock mockClock;
	@Mock
	ProgressCallback mockProgressCallback;
	@Mock
	ProgressingCallable<Integer> mockCallable;
	WriteReadSemaphoreRunner runner;
	String lockKey;
	String readerLockKey;
	String writerLockKey;
	int lockTimeoutSec;
	Integer defaultResults;
	String writeToken;
	String readToken;
	final int maxReaders = 8;
	
	@Before
	public void before() throws Exception{
		// simulate 10 seconds between calls.
		when(mockClock.currentTimeMillis()).thenReturn(0L,1000*10L,1000*20L,1000*30L);
		lockKey = "123";
		readerLockKey = WriteReadSemaphoreRunnerImpl.createReaderLockKey(lockKey);
		writerLockKey = WriteReadSemaphoreRunnerImpl.createWriterLockKey(lockKey);
		lockTimeoutSec = 10;
		runner = new WriteReadSemaphoreRunnerImpl(mockCountingSemaphore, mockClock, maxReaders);
		
		defaultResults = 999;
		// Setup the callable to call progressMade()
		doAnswer(new Answer<Integer>(){

			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable {
				ProgressCallback callback = (ProgressCallback) invocation.getArguments()[0];
				return defaultResults;
			}}).when(mockCallable).call(any(ProgressCallback.class));

		writeToken = "aWriteToken";
		when(mockCountingSemaphore.attemptToAcquireLock(writerLockKey, lockTimeoutSec, WriteReadSemaphoreRunnerImpl.WRITER_MAX_LOCKS)).thenReturn(writeToken);
		
		readToken = "aReadToken";
		when(mockCountingSemaphore.attemptToAcquireLock(readerLockKey, lockTimeoutSec, maxReaders)).thenReturn(readToken);
		
		doAnswer(new Answer<Void>(){

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				ProgressListener listener = (ProgressListener) invocation.getArguments()[0];
				// make progress
				listener.progressMade();
				listener.progressMade();
				listener.progressMade();
				return null;
			}}).when(mockProgressCallback).addProgressListener(any(ProgressListener.class));
	}
	
	@Test
	public void testReadCallalbeHappy() throws Exception{
		// call under test.
		Integer results = runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
		assertEquals(defaultResults, results);
		verify(mockCountingSemaphore).releaseLock(readerLockKey, readToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockCountingSemaphore, times(3)).refreshLockTimeout(readerLockKey, readToken, lockTimeoutSec);
		// verify that the listener is added and removed.
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}

	
	@Test (expected=IllegalArgumentException.class)
	public void testReadCallalbeNullCallback() throws Exception{
		// the passed callback can be null
		mockProgressCallback = null;
		String readToken = "aReadToken";
		when(mockCountingSemaphore.attemptToAcquireLock(readerLockKey, lockTimeoutSec, maxReaders)).thenReturn(readToken);
		// call under test.
		Integer results = runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=LockUnavilableException.class)
	public void testReadCallalbeLockUnavaiable() throws Exception{
		// null token means the lock cannot be acquired.
		when(mockCountingSemaphore.attemptToAcquireLock(readerLockKey, lockTimeoutSec, maxReaders)).thenReturn(null);
		// call under test.
		runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
	}

	@Test
	public void testReadCallalbeWriterAlreadyExists() throws Exception{
		when(mockCountingSemaphore.existsUnexpiredLock(writerLockKey)).thenReturn(true);
		try {
			// call under test.
			runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
			fail();
		} catch (LockUnavilableException e){
			//expected
		}

		verify(mockCountingSemaphore).existsUnexpiredLock(writerLockKey);
		verifyNoMoreInteractions(mockCountingSemaphore);
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
		lockTimeoutSec = WriteReadSemaphoreRunnerImpl.MINIMUM_LOCK_TIMEOUT_SEC-1;
		// call under test.
		runner.tryRunWithReadLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
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
		verify(mockCountingSemaphore).releaseLock(readerLockKey, readToken);
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}
	
	@Test
	public void testTryRunWithWriteLockHappy() throws Exception{
		// call under test.
		Integer results = runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
		assertEquals(defaultResults, results);
		verify(mockCountingSemaphore).releaseLock(writerLockKey, writeToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockCountingSemaphore, times(3)).refreshLockTimeout(writerLockKey, writeToken, lockTimeoutSec);
		// verify that the listener is added and removed.
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));

		//verify we never had to wait for readers to release locks because no readers existed
		verify(mockClock, never()).sleep(anyLong());
		verify(mockCountingSemaphore, never()).refreshLockTimeout(any(), any(), eq(WriteReadSemaphoreRunnerImpl.THROTTLE_SLEEP_FREQUENCY_MS + lockTimeoutSec));
	}

	@Test
	public void testTryRunWithWriteLock_WaitForReaders() throws Exception{
		//force the writer to sleep for 2 cycles waiting for readers to release their locks
		when(mockCountingSemaphore.existsUnexpiredLock(readerLockKey)).thenReturn(true, true, false);

		// call under test.
		Integer results = runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);


		assertEquals(defaultResults, results);
		verify(mockCountingSemaphore).releaseLock(writerLockKey, writeToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockCountingSemaphore, times(3)).refreshLockTimeout(writerLockKey, writeToken, lockTimeoutSec);

		// verify that the listener is added and removed.
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));

		//verify writer waited for readers to finish
		verify(mockClock, times(2)).sleep(WriteReadSemaphoreRunnerImpl.THROTTLE_SLEEP_FREQUENCY_MS);
		verify(mockCountingSemaphore, times(2)).refreshLockTimeout(writerLockKey, writeToken, WriteReadSemaphoreRunnerImpl.THROTTLE_SLEEP_FREQUENCY_MS + lockTimeoutSec);

	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testTryRunWithWriteLockNullCallback() throws Exception{
		mockProgressCallback = null;
		// call under test.
		Integer results = runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
	}
	
	@Test (expected=LockUnavilableException.class)
	public void testTryRunWithWriteLockUnavailable() throws Exception{
		String writeToken = "aWriteToken";
		when(mockCountingSemaphore.attemptToAcquireLock(writerLockKey, lockTimeoutSec, WriteReadSemaphoreRunnerImpl.WRITER_MAX_LOCKS)).thenReturn(null);
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
		lockTimeoutSec = WriteReadSemaphoreRunnerImpl.MINIMUM_LOCK_TIMEOUT_SEC-1;
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
		doThrow(error).when(mockCallable).call(any(ProgressCallback.class));
		// call under test.
		try {
			runner.tryRunWithWriteLock(mockProgressCallback, lockKey, lockTimeoutSec, mockCallable);
			fail("should have failed");
		} catch (Exception e) {
			assertEquals(error, e);
		}
		// the write lock should still be released.
		verify(mockCountingSemaphore).releaseLock(writerLockKey, writeToken);
		// verify that the listener is added and removed even with an exception.
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}
}
