package org.sagebionetworks.workers.util.semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.*;

import java.io.IOException;

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
import org.sagebionetworks.database.semaphore.LockReleaseFailedException;

@ExtendWith(MockitoExtension.class)
public class ReadLockBundleTest {

	@Mock
	private ProgressCallback mockCallback;
	@Mock
	private CountingSemaphore mockCountingSemaphore;
	@Captor
	private ArgumentCaptor<ProgressListener> listenerCaptor;

	private int maxNumberOfReaders;
	private String[] keys;
	private long maxTimeout;

	@BeforeEach
	public void before() {
		maxNumberOfReaders = 4;
		keys = new String[] { "one", "two", "three" };
		maxTimeout = 31L;
	}

	@Test
	public void testConstructorWithNullCallback() {
		mockCallback = null;
		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			new ReadLockBundle(mockCallback, mockCountingSemaphore, 0, "someKey");
		}).getMessage();
		assertEquals("ProgressCallback cannot be null", message);
	}

	@Test
	public void testConstructorWithNullSemaphore() {
		mockCountingSemaphore = null;
		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			new ReadLockBundle(mockCallback, mockCountingSemaphore, 0, "someKey");
		}).getMessage();
		assertEquals("CountingSemaphore cannot be null", message);
	}

	@Test
	public void testConstructorWithNullLockKeys() {
		String[] lockKeys = null;
		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			new ReadLockBundle(mockCallback, mockCountingSemaphore, 0, lockKeys);
		}).getMessage();
		assertEquals("LockKeys cannot be null", message);
	}

	@Test
	public void testConstructorWithEmptyLockKeys() {
		String[] lockKeys = new String[0];
		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			new ReadLockBundle(mockCallback, mockCountingSemaphore, 0, lockKeys);
		}).getMessage();
		assertEquals("Must include at least one lock key", message);
	}

	@Test
	public void testConstructorWithNullInLockKeys() {
		String[] lockKeys = new String[] { "one", null, "two" };
		String message = assertThrows(IllegalArgumentException.class, () -> {
			// call under test
			new ReadLockBundle(mockCallback, mockCountingSemaphore, 0, lockKeys);
		}).getMessage();
		assertEquals("Lock key cannot be null", message);
	}

	@Test
	public void testAcqurieLockAndClose() throws IOException {
		when(mockCountingSemaphore.attemptToAcquireLock(any(), anyLong(), anyInt())).thenReturn("tokenOne", "tokenTwo",
				"tokenThree");
		when(mockCountingSemaphore.existsUnexpiredLock(any())).thenReturn(false, false, false);
		when(mockCallback.getLockTimeoutSeconds()).thenReturn(31L);

		// call under test
		try (ReadLockBundle bundle = new ReadLockBundle(mockCallback, mockCountingSemaphore, maxNumberOfReaders,
				keys)) {
			bundle.acquireAllReadLocks();
		}

		verify(mockCountingSemaphore).existsUnexpiredLock("one_WRITER_LOCK");
		verify(mockCountingSemaphore).existsUnexpiredLock("two_WRITER_LOCK");
		verify(mockCountingSemaphore).existsUnexpiredLock("three_WRITER_LOCK");
		verify(mockCountingSemaphore, times(3)).existsUnexpiredLock(any());

		verify(mockCountingSemaphore).attemptToAcquireLock("one_READER_LOCK", maxTimeout, maxNumberOfReaders);
		verify(mockCountingSemaphore).attemptToAcquireLock("two_READER_LOCK", maxTimeout, maxNumberOfReaders);
		verify(mockCountingSemaphore).attemptToAcquireLock("three_READER_LOCK", maxTimeout, maxNumberOfReaders);
		verify(mockCountingSemaphore, times(3)).attemptToAcquireLock(any(), anyLong(), anyInt());

		verify(mockCallback).addProgressListener(listenerCaptor.capture());
		ProgressListener listener = listenerCaptor.getValue();
		assertNotNull(listener);
		// trigger progress made
		listener.progressMade();
		verify(mockCountingSemaphore).refreshLockTimeout("one_READER_LOCK", "tokenOne", maxTimeout);
		verify(mockCountingSemaphore).refreshLockTimeout("two_READER_LOCK", "tokenTwo", maxTimeout);
		verify(mockCountingSemaphore).refreshLockTimeout("three_READER_LOCK", "tokenThree", maxTimeout);
		verify(mockCountingSemaphore, times(3)).refreshLockTimeout(any(), any(), anyLong());

		// close checks
		verify(mockCallback).removeProgressListener(listener);
		verify(mockCountingSemaphore).releaseLock("one_READER_LOCK", "tokenOne");
		verify(mockCountingSemaphore).releaseLock("two_READER_LOCK", "tokenTwo");
		verify(mockCountingSemaphore).releaseLock("three_READER_LOCK", "tokenThree");
		verify(mockCountingSemaphore, times(3)).releaseLock(any(), any());
	}

	@Test
	public void testAcqurieLockWithUnexpiredLock() {
		when(mockCountingSemaphore.existsUnexpiredLock(any())).thenReturn(false, true, false);

		String message = assertThrows(LockUnavilableException.class, () -> {
			try (ReadLockBundle bundle = new ReadLockBundle(mockCallback, mockCountingSemaphore, maxNumberOfReaders,
					keys)) {
				// call under test
				bundle.acquireAllReadLocks();
			}
		}).getMessage();

		assertEquals("Cannot get an read lock for key: two", message);

		verify(mockCountingSemaphore).existsUnexpiredLock("one_WRITER_LOCK");
		verify(mockCountingSemaphore).existsUnexpiredLock("two_WRITER_LOCK");
		verify(mockCountingSemaphore, never()).existsUnexpiredLock("three_WRITER_LOCK");
		verify(mockCountingSemaphore, times(2)).existsUnexpiredLock(any());

		verifyNoMoreInteractions(mockCountingSemaphore);
		verifyNoMoreInteractions(mockCallback);

	}

	@Test
	public void testAcqurieLockAndCloseWithFailedReadLock() throws IOException {
		// null signals a failed lock attempt.
		when(mockCountingSemaphore.attemptToAcquireLock(any(), anyLong(), anyInt())).thenReturn("tokenOne", "tokenTwo", null);
		when(mockCountingSemaphore.existsUnexpiredLock(any())).thenReturn(false, false, false);
		when(mockCallback.getLockTimeoutSeconds()).thenReturn(31L);

		String message = assertThrows(LockUnavilableException.class, ()->{
			// call under test
			try (ReadLockBundle bundle = new ReadLockBundle(mockCallback, mockCountingSemaphore, maxNumberOfReaders,
					keys)) {
				bundle.acquireAllReadLocks();
			}
		}).getMessage();
		assertEquals("Cannot get an read lock for key: three", message);

		verify(mockCountingSemaphore).existsUnexpiredLock("one_WRITER_LOCK");
		verify(mockCountingSemaphore).existsUnexpiredLock("two_WRITER_LOCK");
		verify(mockCountingSemaphore).existsUnexpiredLock("three_WRITER_LOCK");
		verify(mockCountingSemaphore, times(3)).existsUnexpiredLock(any());

		verify(mockCountingSemaphore).attemptToAcquireLock("one_READER_LOCK", maxTimeout, maxNumberOfReaders);
		verify(mockCountingSemaphore).attemptToAcquireLock("two_READER_LOCK", maxTimeout, maxNumberOfReaders);
		verify(mockCountingSemaphore).attemptToAcquireLock("three_READER_LOCK", maxTimeout, maxNumberOfReaders);
		verify(mockCountingSemaphore, times(3)).attemptToAcquireLock(any(), anyLong(), anyInt());

		verify(mockCallback, never()).addProgressListener(any());

		// close checks
		verify(mockCallback, never()).removeProgressListener(any());
		// the first two locks must be released even though the third lock attempt failed.
		verify(mockCountingSemaphore).releaseLock("one_READER_LOCK", "tokenOne");
		verify(mockCountingSemaphore).releaseLock("two_READER_LOCK", "tokenTwo");
		verify(mockCountingSemaphore, times(2)).releaseLock(any(), any());
	}
	
	@Test
	public void testAcqurieLockAndCloseWithReleaseFailed() throws IOException {
		when(mockCountingSemaphore.attemptToAcquireLock(any(), anyLong(), anyInt())).thenReturn("tokenOne", "tokenTwo",
				"tokenThree");
		when(mockCountingSemaphore.existsUnexpiredLock(any())).thenReturn(false, false, false);
		when(mockCallback.getLockTimeoutSeconds()).thenReturn(31L);
		// release failure on the first lock should still release the other locks.
		LockReleaseFailedException releaseException = new LockReleaseFailedException("failed to release");
		doThrow(releaseException).when(mockCountingSemaphore).releaseLock("one_READER_LOCK", "tokenOne");

		IOException exception = assertThrows(IOException.class, ()->{
			// call under test
			try (ReadLockBundle bundle = new ReadLockBundle(mockCallback, mockCountingSemaphore, maxNumberOfReaders,
					keys)) {
				bundle.acquireAllReadLocks();
			}
		});
		assertEquals(releaseException, exception.getCause());

		verify(mockCountingSemaphore).existsUnexpiredLock("one_WRITER_LOCK");
		verify(mockCountingSemaphore).existsUnexpiredLock("two_WRITER_LOCK");
		verify(mockCountingSemaphore).existsUnexpiredLock("three_WRITER_LOCK");
		verify(mockCountingSemaphore, times(3)).existsUnexpiredLock(any());

		verify(mockCountingSemaphore).attemptToAcquireLock("one_READER_LOCK", maxTimeout, maxNumberOfReaders);
		verify(mockCountingSemaphore).attemptToAcquireLock("two_READER_LOCK", maxTimeout, maxNumberOfReaders);
		verify(mockCountingSemaphore).attemptToAcquireLock("three_READER_LOCK", maxTimeout, maxNumberOfReaders);
		verify(mockCountingSemaphore, times(3)).attemptToAcquireLock(any(), anyLong(), anyInt());

		verify(mockCallback).addProgressListener(listenerCaptor.capture());
		ProgressListener listener = listenerCaptor.getValue();
		assertNotNull(listener);
		// trigger progress made
		listener.progressMade();
		verify(mockCountingSemaphore).refreshLockTimeout("one_READER_LOCK", "tokenOne", maxTimeout);
		verify(mockCountingSemaphore).refreshLockTimeout("two_READER_LOCK", "tokenTwo", maxTimeout);
		verify(mockCountingSemaphore).refreshLockTimeout("three_READER_LOCK", "tokenThree", maxTimeout);
		verify(mockCountingSemaphore, times(3)).refreshLockTimeout(any(), any(), anyLong());

		// close checks
		verify(mockCallback).removeProgressListener(listener);
		verify(mockCountingSemaphore).releaseLock("one_READER_LOCK", "tokenOne");
		verify(mockCountingSemaphore).releaseLock("two_READER_LOCK", "tokenTwo");
		verify(mockCountingSemaphore).releaseLock("three_READER_LOCK", "tokenThree");
		verify(mockCountingSemaphore, times(3)).releaseLock(any(), any());
	}

}
