package org.sagebionetworks.workers.util.semaphore;

/**
 * An abstraction for a provider of one or more locks. The provider should be
 * used in a try-with-resources block to ensure any acquired locks are
 * unconditionally released.
 *
 */
public interface LockProvider extends AutoCloseable {

	/**
	 * Attempt to acquire the lock/locks requested by the provider.
	 * 
	 * @throws LockUnavilableException Thrown if the lock/locks cannot be acquired.
	 */
	void attemptToAcquireLock() throws LockUnavilableException;
}
