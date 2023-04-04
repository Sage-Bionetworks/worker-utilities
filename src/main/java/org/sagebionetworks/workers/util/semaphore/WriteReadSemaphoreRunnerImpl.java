package org.sagebionetworks.workers.util.semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.common.util.Clock;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressListener;
import org.sagebionetworks.common.util.progress.ProgressingCallable;
import org.sagebionetworks.database.semaphore.CountingSemaphore;

public class WriteReadSemaphoreRunnerImpl implements WriteReadSemaphoreRunner {

	private static final Logger log = LogManager
			.getLogger(WriteReadSemaphoreRunnerImpl.class);

	CountingSemaphore countingSemaphore;
	Clock clock;
	final int maxNumberOfReaders;

	/**
	 * Create a new runner for each use.
	 * @param countingSemaphore
	 * @param clock
	 */
	public WriteReadSemaphoreRunnerImpl(CountingSemaphore countingSemaphore, Clock clock, int maxNumberOfReaders) {
		super();
		this.countingSemaphore = countingSemaphore;
		this.clock = clock;
		this.maxNumberOfReaders = maxNumberOfReaders;
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.workers.util.semaphore.WriteReadSemaphoreRunner#tryRunWithWriteLock(java.lang.String, long, org.sagebionetworks.workers.util.progress.ProgressingCallable)
	 */
	@Override
	public <R, T> R tryRunWithWriteLock(final ProgressCallback callback, final String lockKey,
			ProgressingCallable<R> callable) throws Exception {
		if(callback == null){
			throw new IllegalArgumentException("ProgressCallback cannot be null");
		}
		if(lockKey == null){
			throw new IllegalArgumentException("LockKey cannot be null");
		}
		if(callback.getLockTimeoutSeconds() < Constants.MINIMUM_LOCK_TIMEOUT_SEC){
			throw new IllegalArgumentException("LockTimeout cannot be less than 2 seconds");
		}
		if(callable == null){
			throw new IllegalArgumentException("Callable cannot be null");
		}

		final String readerLockKey = Constants.createReaderLockKey(lockKey);
		final String writerLockKey = Constants.createWriterLockKey(lockKey);

		//reserve a writer token if possible
		String writerToken = this.countingSemaphore.attemptToAcquireLock(writerLockKey, callback.getLockTimeoutSeconds(), Constants.WRITER_MAX_LOCKS);
		if(writerToken == null){
			throw new LockUnavilableException("Cannot get n write lock for key:"+lockKey);
		}

		//We have the lockToken, but we must also assure that all readers are done before proceeding.
		while(countingSemaphore.existsUnexpiredLock(readerLockKey)){
			//refresh lock to include the time we sleep waiting for reader to finish
			this.countingSemaphore.refreshLockTimeout(writerLockKey, writerToken,Constants.THROTTLE_SLEEP_FREQUENCY_MS + callback.getLockTimeoutSeconds());
			log.debug("Waiting for reader locks to release on key: "+lockKey+"...");
			clock.sleep(Constants.THROTTLE_SLEEP_FREQUENCY_MS);
		}
		//after waking from sleep and confirming no more readers, the lock should still have <lockTimeoutSec> seconds left before expiring

		// Listen to progress events
		ProgressListener listener = () -> {
			// as progress is made refresh the write lock
			countingSemaphore.refreshLockTimeout(writerLockKey, writerToken, callback.getLockTimeoutSeconds());
		};
		callback.addProgressListener(listener);

		// once we have the write lock we are ready to run
		try{
			return callable.call(callback);
		}finally{
			// unconditionally remove listener.
			callback.removeProgressListener(listener);
			countingSemaphore.releaseLock(writerLockKey, writerToken);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.workers.util.semaphore.WriteReadSemaphoreRunner#tryRunWithReadLock(java.lang.String, long, org.sagebionetworks.workers.util.progress.ProgressingCallable)
	 */
	@Override
	public <R,T> R tryRunWithReadLock(final ProgressCallback callback, final String lockKey,
			final ProgressingCallable<R> callable) throws Exception {
		return tryRunWithReadLock(callback, callable, lockKey);
	}
	
	@Override
	public <R> R tryRunWithReadLock(ProgressCallback callback, ProgressingCallable<R> callable, String... lockKeys)
			throws Exception {
		if(callable == null){
			throw new IllegalArgumentException("Callable cannot be null");
		}
		if(callback == null){
			throw new IllegalArgumentException("Callback cannot be null");
		}
		if(callback.getLockTimeoutSeconds() < Constants.MINIMUM_LOCK_TIMEOUT_SEC){
			throw new IllegalArgumentException("LockTimeout cannot be less than 2 seconds");
		}
		if(lockKeys == null) {
			throw new IllegalArgumentException("lockKeys cannot be null");
		}
		try (ReadLockBundle lockBundle = new ReadLockBundle(callback, countingSemaphore, maxNumberOfReaders, lockKeys)) {
			lockBundle.acquireAllReadLocks();
			return callable.call(callback);
		}
	}

}
