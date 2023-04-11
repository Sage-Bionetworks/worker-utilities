package org.sagebionetworks.workers.util.semaphore;

import org.sagebionetworks.common.util.progress.ProgressingCallable;

/**
 * An abstraction for a semaphore that will run a {@link ProgressingCallable}
 * while holding either a write-lock (exclusive) or read-lock (shared). The
 * locks will be unconditionally released when the runner terminates either
 * normally or with exception.
 * 
 */
public interface WriteReadSemaphore {

	/**
	 * Get a write lock provider for the given request. This method should be called
	 * from within a try-with-resource to ensure the acquired lock is automatically
	 * released, even for failures.
	 * 
	 * @param request
	 * @return
	 * @throws LockUnavilableException Throw if the write lock cannot be acquired.
	 */
	WriteLockProvider getWriteLockProvider(WriteLockRequest request) throws LockUnavilableException;

	/**
	 * Get a read lock provider for the given request. This method should be called
	 * from within a try-with-resource to ensure the acquired lock is automatically
	 * released, even for failures.
	 * 
	 * @param request
	 * @return
	 * @throws LockUnavilableException Thrown if any of the requested read locks
	 *                                 cannot be be acquired.
	 */
	ReadLockProvider getReadLockProvider(ReadLockRequest request) throws LockUnavilableException;
}
