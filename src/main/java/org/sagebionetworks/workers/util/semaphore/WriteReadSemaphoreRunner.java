package org.sagebionetworks.workers.util.semaphore;

import org.sagebionetworks.workers.util.progress.ProgressCallback;
import org.sagebionetworks.workers.util.progress.ProgressingCallable;

/**
 * An abstraction for a semaphore that will run a {@link ProgressingCallable}
 * while holding either a write-lock (exclusive) or read-lock (shared). The
 * locks will be unconditionally released when the runner terminates either
 * normally or with exception.
 * 
 */
public interface WriteReadSemaphoreRunner {

	/**
	 * <p>
	 * The passed callable will be run while holding the exclusive lock on the
	 * passed resource. The lock will be unconditionally released when the
	 * runner terminates either normally or with exception.
	 * </p>
	 * <p>
	 * Note: In order to acquire a write-lock this method might need to wait for
	 * outstanding read-locks to be released.
	 * </p>
	 * 
	 * @param callback
	 *            Optional (can be null), when provided, any progress made by
	 *            the callbale will be forwarded to this callback.
	 * @param lockKey
	 *            The key that identifies the resource to lock on.
	 * @param lockTimeoutSec
	 *            The maximum number of seconds that the lock will be held for.
	 *            This must be greater than the amount of time the passed runner
	 *            is expected to run.
	 * @param callable
	 *            The call() method of this runner will be called while the lock
	 *            is being held.
	 * @return
	 * @throws LockUnavilableException
	 *             Thrown if the requested lock cannot be acquired for any
	 *             reason.
	 * @throws InterruptedException
	 *             Thrown if the waiting gets interrupted.
	 * @throws Exception
	 */
	public <T> T tryRunWithWriteLock(ProgressCallback<T> callback,
			String lockKey, long lockTimeoutSec, ProgressingCallable<T> callable)
			throws Exception;

	/**
	 * <p>
	 * The passed callable will be run while holding a read-lock (shared) on the
	 * passed resource. The lock will be unconditionally released when the
	 * runner terminates either normally or with exception.
	 * </p>
	 * 
	 * @param callback
	 *            Optional (can be null), when provided, any progress made by
	 *            the callbale will be forwarded to this callback.
	 * @param lockKey
	 *            The key that identifies the resource to lock on.
	 * @param lockTimeoutSec
	 *            The maximum number of seconds that the lock will be held for.
	 *            This must be greater than the amount of time the passed runner
	 *            is expected to run.
	 * @param runner
	 *            The call() method of this runner will be called while the lock
	 *            is being held.
	 * @return
	 * @throws LockUnavilableException
	 *             Thrown if the requested lock cannot be acquired for any
	 *             reason.
	 * @throws Exception
	 */
	public <T> T tryRunWithReadLock(ProgressCallback<T> callback,
			String lockKey, long lockTimeoutSec, ProgressingCallable<T> runner)
			throws Exception;

}
