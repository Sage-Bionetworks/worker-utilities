package org.sagebionetworks.workers.util.semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.common.util.progress.AutoProgressingRunner;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressListener;
import org.sagebionetworks.common.util.progress.ProgressingRunner;
import org.sagebionetworks.common.util.progress.ThrottlingProgressCallback;
import org.sagebionetworks.database.semaphore.CountingSemaphore;
import org.sagebionetworks.database.semaphore.LockReleaseFailedException;

/**
 * This is not a singleton. A new instance of this gate must be created each
 * time you need one.
 * 
 */
public class SemaphoreGatedRunnerImpl implements SemaphoreGatedRunner {

	private static final Logger log = LogManager
			.getLogger(SemaphoreGatedRunnerImpl.class);

	final CountingSemaphore semaphore;
	final ProgressingRunner<Void> runner;
	final String lockKey;
	final long lockTimeoutSec;
	final int maxLockCount;
	final long throttleFrequencyMS;
	final boolean useProgressHeartbeat;

	/**
	 * 
	 * @param semaphore
	 *            A live database semaphore.
	 * @param config
	 *            configuration for this instances.
	 */
	public SemaphoreGatedRunnerImpl(CountingSemaphore semaphore,
			SemaphoreGatedRunnerConfiguration config) {
		super();
		this.semaphore = semaphore;
		if (config == null) {
			throw new IllegalArgumentException("Configuration cannot be null");
		}

		this.lockKey = config.lockKey;
		this.lockTimeoutSec = config.getLockTimeoutSec();
		this.maxLockCount = config.getMaxLockCount();
		// the frequency that {@link ProgressCallback#progressMade(Object)}
		// calls can refresh the lock in the DB.
		this.throttleFrequencyMS = (this.lockTimeoutSec * 1000) / 3;
		this.useProgressHeartbeat = config.useProgressHeartbeat();
		if(this.useProgressHeartbeat){
			// wrap the runner to generate progress heartbeats.
			this.runner = new AutoProgressingRunner(config.getRunner(), this.throttleFrequencyMS);
		}else{
			this.runner = config.getRunner();
		}
		validateConfig();
	}

	/**
	 * This is the run of the 'runnable'
	 */
	public void run() {
		try {
			// attempt to get a lock
			final String lockToken = semaphore.attemptToAcquireLock(
					this.lockKey, this.lockTimeoutSec, this.maxLockCount);
			// start with a new callback.
			ProgressCallback<Void> progressCallback = new ThrottlingProgressCallback<Void>(this.throttleFrequencyMS);
			// listen to progress events
			progressCallback.addProgressListener(new ProgressListener<Void>() {

				@Override
				public void progressMade(Void t) {
					// Give the lock more time
					semaphore.refreshLockTimeout(lockKey,
							lockToken, lockTimeoutSec);
				}
			});

			// Only proceed if a lock was acquired
			if (lockToken != null) {
				try {
					// Let the runner go while holding the lock
					runner.run(progressCallback);
				} finally {
					semaphore.releaseLock(this.lockKey, lockToken);
				}
			}
		}catch (LockReleaseFailedException e){
			/*
			 * Lock release failures usually mean workers are not correctly
			 * reporting progress and the semaphore is not working as expected
			 * so this exception is thrown
			 */
			throw e;
		}catch (Throwable e) {
			log.error("Error on key " + lockKey + ": ",e);
		}
	}

	private void validateConfig() {
		if (this.runner == null) {
			throw new IllegalArgumentException("Runner cannot be be null");
		}
		if (this.lockKey == null) {
			throw new IllegalArgumentException("Lock key cannot be null");
		}
		if (lockTimeoutSec < 1) {
			throw new IllegalArgumentException(
					"LockTimeoutSec cannot be less than one.");
		}
		if (maxLockCount < 1) {
			throw new IllegalArgumentException(
					"MaxLockCount cannot be less than one.");
		}
	}

}
