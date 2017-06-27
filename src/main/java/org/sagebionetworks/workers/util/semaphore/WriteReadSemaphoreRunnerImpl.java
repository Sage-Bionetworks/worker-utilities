package org.sagebionetworks.workers.util.semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.common.util.Clock;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressListener;
import org.sagebionetworks.common.util.progress.ProgressingCallable;
import org.sagebionetworks.common.util.progress.ThrottlingProgressCallback;
import org.sagebionetworks.database.semaphore.WriteReadSemaphore;

public class WriteReadSemaphoreRunnerImpl implements WriteReadSemaphoreRunner {

	public static final int MINIMUM_LOCK_TIMEOUT_SEC = 2;
	
	/**
	 * Sleep and throttle frequency.
	 */
	public static final long THROTTLE_SLEEP_FREQUENCY_MS = 2000;

	private static final Logger log = LogManager
			.getLogger(WriteReadSemaphoreRunnerImpl.class);
	
	WriteReadSemaphore writeReadSemaphore;
	Clock clock;
	
	/**
	 * Create a new runner for each use.
	 * @param writeReadSemaphore
	 * @param clock
	 */
	public WriteReadSemaphoreRunnerImpl(WriteReadSemaphore writeReadSemaphore, Clock clock) {
		super();
		this.writeReadSemaphore = writeReadSemaphore;
		this.clock = clock;
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.workers.util.semaphore.WriteReadSemaphoreRunner#tryRunWithWriteLock(java.lang.String, long, org.sagebionetworks.workers.util.progress.ProgressingCallable)
	 */
	@Override
	public <R, T> R tryRunWithWriteLock(final ProgressCallback<T> callback, final String lockKey, final int lockTimeoutSec,
			ProgressingCallable<R,T> callable) throws Exception {
		if(callback == null){
			throw new IllegalArgumentException("ProgressCallback cannot be null");
		}
		if(lockKey == null){
			throw new IllegalArgumentException("LockKey cannot be null");
		}
		if(lockTimeoutSec < MINIMUM_LOCK_TIMEOUT_SEC){
			throw new IllegalArgumentException("LockTimeout cannot be less than 2 seconds");
		}
		if(callable == null){
			throw new IllegalArgumentException("Callable cannot be null");
		}
		
		String precursorToken = this.writeReadSemaphore.acquireWriteLockPrecursor(lockKey, lockTimeoutSec);
		if(precursorToken == null){
			throw new LockUnavilableException("Cannot get an write lock for key:"+lockKey);
		}
		// while holding the precursor attempt to get the write lock
		String writeToken = null;
		while(writeToken == null){
			writeToken = this.writeReadSemaphore.acquireWriteLock(lockKey, precursorToken, lockTimeoutSec);
			if(writeToken == null){
				log.debug("Waiting for write lock on key: "+lockKey+"...");
				clock.sleep(THROTTLE_SLEEP_FREQUENCY_MS);
			}
		}
		final String finalWriteToken = writeToken;
		// Listen to progress events
		callback.addProgressListener(new ProgressListener<T>() {

			@Override
			public void progressMade(T t) {
				// as progress is made refresh the write lock
				writeReadSemaphore.refreshWriteLock(lockKey, finalWriteToken, lockTimeoutSec);
			}
		});

		// once we have the write lock we are ready to run
		try{
			return callable.call(callback);
		}finally{
			if(writeToken != null){
				writeReadSemaphore.releaseWriteLock(lockKey, writeToken);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.workers.util.semaphore.WriteReadSemaphoreRunner#tryRunWithReadLock(java.lang.String, long, org.sagebionetworks.workers.util.progress.ProgressingCallable)
	 */
	@Override
	public <R,T> R tryRunWithReadLock(final ProgressCallback<T> callback, final String lockKey, final int lockTimeoutSec,
			final ProgressingCallable<R,T> callable) throws Exception {
		if(callback == null){
			throw new IllegalArgumentException("ProgressCallback cannot be null");
		}
		if(lockKey == null){
			throw new IllegalArgumentException("LockKey cannot be null");
		}
		if(lockTimeoutSec < MINIMUM_LOCK_TIMEOUT_SEC){
			throw new IllegalArgumentException("LockTimeout cannot be less than 2 seconds");
		}
		if(callable == null){
			throw new IllegalArgumentException("Callable cannot be null");
		}
		final String readToken = this.writeReadSemaphore.acquireReadLock(lockKey, lockTimeoutSec);
		if(readToken == null){
			throw new LockUnavilableException("Cannot get an read lock for key:"+lockKey);
		}
		// listen to callback events
		ProgressListener<T> listener = new ProgressListener<T>() {

			@Override
			public void progressMade(T t) {
				// refresh the read lock as progress is made.
				writeReadSemaphore.refreshReadLock(lockKey, readToken, lockTimeoutSec);
			}
		};
		callback.addProgressListener(listener);
		
		try{
			return callable.call(callback);
		}finally{
			// unconditionally remove the listener.
			callback.removeProgressListener(listener);
			this.writeReadSemaphore.releaseReadLock(lockKey, readToken);
		}
	}

}
