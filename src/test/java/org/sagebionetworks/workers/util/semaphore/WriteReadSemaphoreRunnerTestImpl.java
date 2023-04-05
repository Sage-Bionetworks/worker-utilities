package org.sagebionetworks.workers.util.semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.sagebionetworks.common.util.Clock;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressListener;
import org.sagebionetworks.common.util.progress.ProgressingCallable;
import org.sagebionetworks.database.semaphore.CountingSemaphore;

@ExtendWith(MockitoExtension.class)
public class WriteReadSemaphoreRunnerTestImpl {
	
	@Mock
	private CountingSemaphore mockCountingSemaphore;
	@Mock
	private Clock mockClock;
	@Mock
	private ProgressCallback mockProgressCallback;
	@Mock
	private ProgressingCallable<Integer> mockCallable;
	private WriteReadSemaphoreRunner runner;
	private String lockKey;
	private String readerLockKey;
	private String writerLockKey;
	private long lockTimeoutSec;
	private Integer defaultResults;
	private String writeToken;
	private String readToken;
	private final int maxReaders = 8;
	
	@BeforeEach
	public void before() throws Exception{
		lockKey = "123";
		readerLockKey = Constants.createReaderLockKey(lockKey);
		writerLockKey = Constants.createWriterLockKey(lockKey);
		lockTimeoutSec = 10L;
		runner = new WriteReadSemaphoreRunnerImpl(mockCountingSemaphore, mockClock, maxReaders);
		defaultResults = 999;
		writeToken = "aWriteToken";
		readToken = "aReadToken";
		
	}
	
	private void setupCallbackReturn() throws Exception {
		doAnswer(new Answer<Integer>(){
			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable {
				return defaultResults;
			}}).when(mockCallable).call(any(ProgressCallback.class));
	}
	
	private void setupCallbackAddProgressListener() {
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
		when(mockProgressCallback.getLockTimeoutSeconds()).thenReturn(lockTimeoutSec);
		runner = new WriteReadSemaphoreRunnerImpl(mockCountingSemaphore, mockClock, maxReaders);
		setupCallbackReturn();

		when(mockCountingSemaphore.attemptToAcquireLock(readerLockKey, lockTimeoutSec, maxReaders)).thenReturn(readToken);

		setupCallbackAddProgressListener();
		
		// call under test.
		Integer results = runner.tryRunWithReadLock(mockProgressCallback, lockKey, mockCallable);
		assertEquals(defaultResults, results);
		verify(mockCountingSemaphore).releaseLock(readerLockKey, readToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockCountingSemaphore, times(3)).refreshLockTimeout(readerLockKey, readToken, lockTimeoutSec);
		// verify that the listener is added and removed.
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}
	
	@Test
	public void testReadCallalbeNullCallback() throws Exception{			
		// the passed callback can be null
		mockProgressCallback = null;

		assertThrows(IllegalArgumentException.class, ()->{
			// call under test.
			runner.tryRunWithReadLock(mockProgressCallback, lockKey, mockCallable);
		});

	}
	
	@Test
	public void testReadCallalbeLockUnavaiable() throws Exception{
		when(mockProgressCallback.getLockTimeoutSeconds()).thenReturn(lockTimeoutSec);
		runner = new WriteReadSemaphoreRunnerImpl(mockCountingSemaphore, mockClock, maxReaders);
		when(mockCountingSemaphore.attemptToAcquireLock(readerLockKey, lockTimeoutSec, maxReaders)).thenReturn(readToken);

		// null token means the lock cannot be acquired.
		when(mockCountingSemaphore.attemptToAcquireLock(readerLockKey, lockTimeoutSec, maxReaders)).thenReturn(null);
		
		assertThrows(LockUnavilableException.class, ()->{
			// call under test.
			runner.tryRunWithReadLock(mockProgressCallback, lockKey, mockCallable);
		});

	}

	@Test
	public void testReadCallalbeWriterAlreadyExists() throws Exception{

		when(mockProgressCallback.getLockTimeoutSeconds()).thenReturn(lockTimeoutSec);
		runner = new WriteReadSemaphoreRunnerImpl(mockCountingSemaphore, mockClock, maxReaders);
		when(mockCountingSemaphore.existsUnexpiredLock(writerLockKey)).thenReturn(true);
		
		assertThrows(LockUnavilableException.class, ()->{
			// call under test.
			runner.tryRunWithReadLock(mockProgressCallback, lockKey, mockCallable);
		});

		verify(mockCountingSemaphore).existsUnexpiredLock(writerLockKey);
		verifyNoMoreInteractions(mockCountingSemaphore);
	}

	
	@Test
	public void testReadCallalbeNullKey() throws Exception{
		lockKey = null;
		
		assertThrows(IllegalArgumentException.class, ()->{
			// call under test.
			runner.tryRunWithReadLock(mockProgressCallback, lockKey, mockCallable);
		});
	}
	
	@Test
	public void testReadCallalbeNullCallable() throws Exception{
		mockCallable = null;

		assertThrows(IllegalArgumentException.class, ()->{
			// call under test.
			runner.tryRunWithReadLock(mockProgressCallback, lockKey, mockCallable);
		});

	}
	
	@Test
	public void testReadCallalbeSmallTimeout() throws Exception{
		lockTimeoutSec = Constants.MINIMUM_LOCK_TIMEOUT_SEC-1;
		when(mockProgressCallback.getLockTimeoutSeconds()).thenReturn(lockTimeoutSec);

		assertThrows(IllegalArgumentException.class, ()->{
			// call under test.
			runner.tryRunWithReadLock(mockProgressCallback, lockKey, mockCallable);
		});

	}
	
	
	@Test
	public void testReadCallalbeRemoveListenerException() throws Exception{

		when(mockProgressCallback.getLockTimeoutSeconds()).thenReturn(lockTimeoutSec);
		runner = new WriteReadSemaphoreRunnerImpl(mockCountingSemaphore, mockClock, maxReaders);
		when(mockCountingSemaphore.attemptToAcquireLock(readerLockKey, lockTimeoutSec, maxReaders)).thenReturn(readToken);

		setupCallbackAddProgressListener();
		
		Exception error = new RuntimeException("Failed");
		doThrow(error).when(mockCallable).call(any(ProgressCallback.class));
		
		assertThrows(RuntimeException.class, ()->{
			// call under test.
			runner.tryRunWithReadLock(mockProgressCallback, lockKey, mockCallable);
		});
		
		// lock should be release even for an error
		verify(mockCountingSemaphore).releaseLock(readerLockKey, readToken);
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}
	
	@Test
	public void testTryRunWithWriteLockHappy() throws Exception{

		when(mockProgressCallback.getLockTimeoutSeconds()).thenReturn(lockTimeoutSec);
		runner = new WriteReadSemaphoreRunnerImpl(mockCountingSemaphore, mockClock, maxReaders);
		setupCallbackReturn();

		when(mockCountingSemaphore.attemptToAcquireLock(writerLockKey, lockTimeoutSec, Constants.WRITER_MAX_LOCKS)).thenReturn(writeToken);

		setupCallbackAddProgressListener();
		// call under test.
		Integer results = runner.tryRunWithWriteLock(mockProgressCallback, lockKey, mockCallable);
		assertEquals(defaultResults, results);
		verify(mockCountingSemaphore).releaseLock(writerLockKey, writeToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockCountingSemaphore, times(3)).refreshLockTimeout(writerLockKey, writeToken, lockTimeoutSec);
		// verify that the listener is added and removed.
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));

		//verify we never had to wait for readers to release locks because no readers existed
		verify(mockClock, never()).sleep(anyLong());
		verify(mockCountingSemaphore, times(3)).refreshLockTimeout(writerLockKey, writeToken, lockTimeoutSec);
	}

	@Test
	public void testTryRunWithWriteLock_WaitForReaders() throws Exception{

		when(mockProgressCallback.getLockTimeoutSeconds()).thenReturn(lockTimeoutSec);
		setupCallbackReturn();

		when(mockCountingSemaphore.attemptToAcquireLock(writerLockKey, lockTimeoutSec, Constants.WRITER_MAX_LOCKS)).thenReturn(writeToken);

		setupCallbackAddProgressListener();
		
		//force the writer to sleep for 2 cycles waiting for readers to release their locks
		when(mockCountingSemaphore.existsUnexpiredLock(readerLockKey)).thenReturn(true, true, false);

		// call under test.
		Integer results = runner.tryRunWithWriteLock(mockProgressCallback, lockKey, mockCallable);


		assertEquals(defaultResults, results);
		verify(mockCountingSemaphore).releaseLock(writerLockKey, writeToken);
		verify(mockCallable).call(any(ProgressCallback.class));
		verify(mockCountingSemaphore, times(5)).refreshLockTimeout(writerLockKey, writeToken, lockTimeoutSec);

		// verify that the listener is added and removed.
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));

		//verify writer waited for readers to finish
		verify(mockClock, times(2)).sleep(Constants.THROTTLE_SLEEP_FREQUENCY_MS);
		verify(mockCountingSemaphore, times(5)).refreshLockTimeout(writerLockKey, writeToken, lockTimeoutSec);

	}
	
	@Test
	public void testTryRunWithWriteLockNullCallback() throws Exception {
		mockProgressCallback = null;

		assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			runner.tryRunWithWriteLock(mockProgressCallback, lockKey, mockCallable);
		});

	}
	
	@Test
	public void testTryRunWithWriteLockUnavailable() throws Exception{

		when(mockProgressCallback.getLockTimeoutSeconds()).thenReturn(lockTimeoutSec);
		runner = new WriteReadSemaphoreRunnerImpl(mockCountingSemaphore, mockClock, maxReaders);
		when(mockCountingSemaphore.attemptToAcquireLock(writerLockKey, lockTimeoutSec, Constants.WRITER_MAX_LOCKS)).thenReturn(writeToken);
		when(mockCountingSemaphore.attemptToAcquireLock(writerLockKey, lockTimeoutSec, Constants.WRITER_MAX_LOCKS)).thenReturn(null);

		assertThrows(LockUnavilableException.class, () -> {
			// call under test
			runner.tryRunWithWriteLock(mockProgressCallback, lockKey, mockCallable);
		});

	}

	@Test
	public void testTryRunWithWriteLockNullKey() throws Exception{
		lockKey = null;
	
		assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			runner.tryRunWithWriteLock(mockProgressCallback, lockKey, mockCallable);
		});
	}
	
	@Test
	public void testTryRunWithWriteLockSmallTimeout() throws Exception{
		lockTimeoutSec = Constants.MINIMUM_LOCK_TIMEOUT_SEC-1;
		when(mockProgressCallback.getLockTimeoutSeconds()).thenReturn(lockTimeoutSec);
		assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			runner.tryRunWithWriteLock(mockProgressCallback, lockKey, mockCallable);
		});
	}
	
	@Test
	public void testTryRunWithWriteLockNullCallable() throws Exception{
		mockCallable = null;
		assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			runner.tryRunWithWriteLock(mockProgressCallback, lockKey, mockCallable);
		});
	}
	
	@Test
	public void testTryRunWithWriteException() throws Exception{

		when(mockProgressCallback.getLockTimeoutSeconds()).thenReturn(lockTimeoutSec);
		runner = new WriteReadSemaphoreRunnerImpl(mockCountingSemaphore, mockClock, maxReaders);
		when(mockCountingSemaphore.attemptToAcquireLock(writerLockKey, lockTimeoutSec, Constants.WRITER_MAX_LOCKS)).thenReturn(writeToken);

		setupCallbackAddProgressListener();
		
		Exception error = new RuntimeException("An error");
		doThrow(error).when(mockCallable).call(any(ProgressCallback.class));
		assertThrows(RuntimeException.class, () -> {
			// call under test
			runner.tryRunWithWriteLock(mockProgressCallback, lockKey, mockCallable);
		});
		// the write lock should still be released.
		verify(mockCountingSemaphore).releaseLock(writerLockKey, writeToken);
		// verify that the listener is added and removed even with an exception.
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}
}
