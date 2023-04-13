package org.sagebionetworks.workers.util.semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressListener;
import org.sagebionetworks.database.semaphore.CountingSemaphore;

@ExtendWith(MockitoExtension.class)
public class WriteReadSemaphoreImplTest {

	@Mock
	private ProgressCallback mockCallback;
	@Mock
	private CountingSemaphore mockCountingSemaphore;
	@Captor
	private ArgumentCaptor<ProgressListener> listenerCaptor;

	private WriteReadSemaphoreImpl semaphore;

	private int maxNumberOfReaders;
	private String[] keys;
	private long maxTimeout;
	private String context;

	@BeforeEach
	public void before() {
		maxNumberOfReaders = 4;
		keys = new String[] { "one" };
		maxTimeout = 31L;
		context = "some context";

		semaphore = new WriteReadSemaphoreImpl(mockCountingSemaphore, maxNumberOfReaders);
	}

	@Test
	public void testSemaphoreWithNullCounting() {
		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			new WriteReadSemaphoreImpl(null, maxNumberOfReaders);
		}).getMessage();
		assertEquals("CountingSemaphore cannot be null", message);
	}

	@Test
	public void testGetReadLockProviderWithNullRequest() {
		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			semaphore.getReadLock(null);
		}).getMessage();
		assertEquals("Request cannot be null", message);
	}
	
	@Test
	public void testGetWriteLockProviderWithNullRequest() {
		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			semaphore.getWriteLock(null);
		}).getMessage();
		assertEquals("Request cannot be null", message);
	}

	@Test
	public void testGetReadLockProvider() throws Exception {
		
		when(mockCountingSemaphore.attemptToAcquireLock(any(), anyLong(), anyInt(), any())).thenReturn(
				Optional.of("tokenOne"));
		when(mockCountingSemaphore.getFirstUnexpiredLockContext(any())).thenReturn(Optional.empty());
		when(mockCallback.getLockTimeoutSeconds()).thenReturn(maxTimeout);

		// call under test
		try (ReadLock provider = semaphore
				.getReadLock(new ReadLockRequest(mockCallback, context, keys))) {
		}
		
		verify(mockCountingSemaphore).getFirstUnexpiredLockContext("one_WRITER_LOCK");
		verify(mockCountingSemaphore, times(1)).getFirstUnexpiredLockContext(any());

		verify(mockCountingSemaphore).attemptToAcquireLock("one_READER_LOCK", maxTimeout, maxNumberOfReaders, context);
		verify(mockCountingSemaphore, times(1)).attemptToAcquireLock(any(), anyLong(), anyInt(),any());

		verify(mockCallback).addProgressListener(listenerCaptor.capture());
		ProgressListener listener = listenerCaptor.getValue();
		assertNotNull(listener);
		// trigger progress made
		listener.progressMade();
		verify(mockCountingSemaphore).refreshLockTimeout("one_READER_LOCK", "tokenOne", maxTimeout);
		verify(mockCountingSemaphore, times(1)).refreshLockTimeout(any(), any(), anyLong());

		// close checks
		verify(mockCallback).removeProgressListener(listener);
		verify(mockCountingSemaphore).releaseLock("one_READER_LOCK", "tokenOne");
		verify(mockCountingSemaphore, times(1)).releaseLock(any(), any());

	}

	@Test
	public void testGetWriteLockProvider() throws Exception {
		
		when(mockCountingSemaphore.attemptToAcquireLock(any(), anyLong(), anyInt(), any())).thenReturn(
				Optional.of("tokenOne"));
		when(mockCountingSemaphore.getFirstUnexpiredLockContext(any())).thenReturn(Optional.empty());
		when(mockCallback.getLockTimeoutSeconds()).thenReturn(maxTimeout);

		// call under test
		try (WriteLock provider = semaphore
				.getWriteLock(new WriteLockRequest(mockCallback, context, keys[0]))) {
			provider.getExistingReadLockContext();
			provider.getExistingReadLockContext();
			provider.getExistingReadLockContext();
		}
		verify(mockCountingSemaphore).attemptToAcquireLock("one_WRITER_LOCK", maxTimeout, Constants.WRITER_MAX_LOCKS, context);
		verify(mockCountingSemaphore, times(1)).attemptToAcquireLock(any(), anyLong(), anyInt(), any());
		
		verify(mockCountingSemaphore, times(3)).getFirstUnexpiredLockContext("one_READER_LOCK");
		verify(mockCountingSemaphore, times(3)).getFirstUnexpiredLockContext(any());
		
		verify(mockCallback).addProgressListener(listenerCaptor.capture());
		ProgressListener listener = listenerCaptor.getValue();
		assertNotNull(listener);
		// trigger progress made
		listener.progressMade();
		listener.progressMade();
		verify(mockCountingSemaphore, times(2)).refreshLockTimeout("one_WRITER_LOCK", "tokenOne", maxTimeout);
		verify(mockCountingSemaphore, times(2)).refreshLockTimeout(any(), any(), anyLong());

		// close checks
		verify(mockCallback).removeProgressListener(listener);
		verify(mockCountingSemaphore).releaseLock("one_WRITER_LOCK", "tokenOne");
		verify(mockCountingSemaphore, times(1)).releaseLock(any(), any());

	}
}
