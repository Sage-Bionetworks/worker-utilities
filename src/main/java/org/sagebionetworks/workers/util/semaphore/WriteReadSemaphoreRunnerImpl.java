package org.sagebionetworks.workers.util.semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.database.semaphore.WriteReadSemaphore;
import org.sagebionetworks.workers.util.Clock;
import org.sagebionetworks.workers.util.progress.ProgressCallback;
import org.sagebionetworks.workers.util.progress.ProgressingCallable;
import org.sagebionetworks.workers.util.progress.ThrottlingProgressCallback;

public class WriteReadSemaphoreRunnerImpl implements WriteReadSemaphoreRunner {

	private static final Logger log = LogManager
			.getLogger(SemaphoreGatedRunnerImpl.class);
	
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
	public <T> T tryRunWithWriteLock(final ProgressCallback<T> callback, final String lockKey, final long lockTimeoutSec,
			ProgressingCallable<T> callable) throws Exception {
		if(lockKey == null){
			throw new IllegalArgumentException("LockKey cannot be null");
		}
		if(lockTimeoutSec < 2){
			throw new IllegalArgumentException("LockTimeout cannot be less than 2 seconds");
		}
		long halfTimeoutMs = (lockTimeoutSec/2)*1000;
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
				clock.sleep(halfTimeoutMs);
			}
		}
		final String finalWriteToken = writeToken;
		// once we have the write lock we are ready to run
		try{
			return callable.call((new ThrottlingProgressCallback<T>(new ProgressCallback<T>() {
				@Override
				public void progressMade(T t) {
					// as progress is made refresh the write lock
					writeReadSemaphore.refreshWriteLock(lockKey, finalWriteToken, lockTimeoutSec);
					// forward if a callabck was provided.
					if(callback != null){
						callback.progressMade(t);
					}
				}
			}, halfTimeoutMs, clock)));
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
	public <T> T tryRunWithReadLock(final ProgressCallback<T> callback, final String lockKey, final long lockTimeoutSec,
			final ProgressingCallable<T> callable) throws Exception {
		if(lockKey == null){
			throw new IllegalArgumentException("LockKey cannot be null");
		}
		if(lockTimeoutSec < 2){
			throw new IllegalArgumentException("LockTimeout cannot be less than 2 seconds");
		}
		long halfTimeoutMs = (lockTimeoutSec/2)*1000;
		if(callable == null){
			throw new IllegalArgumentException("Callable cannot be null");
		}
		final String readToken = this.writeReadSemaphore.acquireReadLock(lockKey, lockTimeoutSec);
		if(readToken == null){
			throw new LockUnavilableException("Cannot get an write lock for key:"+lockKey);
		}
		try{
			return callable.call(new ThrottlingProgressCallback<T>(new ProgressCallback<T>() {

				@Override
				public void progressMade(T t) {
					// refresh the read lock as progress is made.
					writeReadSemaphore.refreshReadLock(lockKey, readToken, lockTimeoutSec);
					if(callback != null){
						callback.progressMade(t);
					}
				}
			}, halfTimeoutMs, clock));
		}finally{
			this.writeReadSemaphore.releaseReadLock(lockKey, readToken);
		}
	}

}
