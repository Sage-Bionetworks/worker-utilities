package org.sagebionetworks.workers.util.semaphore;

import java.util.Optional;

/**
 * Represents a provider of an exclusive write lock.
 * <p>
 * Note: This provider must be used with try-with-resources to ensure that the
 * lock is unconditionally released.
 *
 */
public interface WriteLockProvider extends LockProvider {

	/**
	 * If there are any existing read locks associated with this write lock then
	 * return the context of the first read lock.
	 * 
	 * @return The context string of the first existing read lock.
	 *         {@link Optional#empty()} if there are no outstanding read locks.
	 */
	Optional<String> getExistingReadLockContext();
}
