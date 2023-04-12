package org.sagebionetworks.workers.util.semaphore;

/**
 * An abstraction to provide either shared read locks or exclusive write locks.
 * 
 */
public interface WriteReadSemaphore {

	/**
	 * Get a write lock provider for the given request. This method should be called
	 * from within a try-with-resource to ensure the acquired lock is automatically
	 * released, even for failures. For example:
	 * <pre>
		try(WriteLockProvider provider = writeReadSemaphore.getWriteLockProvider(request)){
			// first get the write lock
			provider.attemptToAcquireLock();
			// then wait for the readers to release their locks
			Optional<String> readerContextOption;
			while((readerContextOption = provider.getExistingReadLockContext()).isPresent()) {
				log.info("Waiting for read lock to be released: "+readerContextOption.get());
				Thread.sleep(2000);
			}
			// code to execute while holding the lock added here...
		}
	 * </pre>
	 * 
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
	 * <pre>
		try(ReadLockProvider provider = writeReadSemaphore.getReadLockProvider(request)){
			// first get the write lock
			provider.attemptToAcquireLock();
			// code to execute while holding the lock added here...
		}
	 * </pre>
	 * @param request
	 * @return
	 * @throws LockUnavilableException Thrown if any of the requested read locks
	 *                                 cannot be be acquired.
	 */
	ReadLockProvider getReadLockProvider(ReadLockRequest request) throws LockUnavilableException;
}
