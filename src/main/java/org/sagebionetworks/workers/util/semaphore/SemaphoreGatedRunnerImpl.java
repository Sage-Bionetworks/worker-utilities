package org.sagebionetworks.workers.util.semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.common.util.progress.AutoProgressingRunner;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressListener;
import org.sagebionetworks.common.util.progress.ProgressingRunner;
import org.sagebionetworks.common.util.progress.SynchronizedProgressCallback;
import org.sagebionetworks.database.semaphore.CountingSemaphore;
import org.sagebionetworks.database.semaphore.LockReleaseFailedException;
import org.sagebionetworks.workers.util.Gate;

/**
 * This is not a singleton. A new instance of this gate must be created each
 * time you need one.
 * 
 */
public class SemaphoreGatedRunnerImpl implements SemaphoreGatedRunner {

	private static final Logger log = LogManager
			.getLogger(SemaphoreGatedRunnerImpl.class);

	final CountingSemaphore semaphore;
	final ProgressingRunner runner;
	final String lockKey;
	final long lockTimeoutSec;
	final int maxLockCount;
	final long heartBeatFrequencyMS;
	final Gate gate;

	/**
	 * 
	 * @param semaphore
	 *            A live database semaphore.
	 * @param config
	 *            configuration for this instances.
	 */
	public SemaphoreGatedRunnerImpl(CountingSemaphore semaphore,
			SemaphoreGatedRunnerConfiguration config, Gate gate) {

		super();
		this.gate = gate;
		this.semaphore = semaphore;
		if (config == null) {
			throw new IllegalArgumentException("Configuration cannot be null");
		}

		this.lockKey = config.lockKey;
		this.lockTimeoutSec = config.getLockTimeoutSec();
		this.maxLockCount = config.getMaxLockCount();
		// the frequency that {@link ProgressCallback#progressMade(Object)}
		// calls can refresh the lock in the DB.
		this.heartBeatFrequencyMS = (this.lockTimeoutSec * 1000) / 3;
		this.runner = new AutoProgressingRunner(config.getRunner(), this.heartBeatFrequencyMS);
		validateConfig();
	}

	/**
	 * This is the run of the 'runnable'
	 */
	public void run() {
		try {
			if(!canRun()){
				return;
			}

			// attempt to get a lock
			final String lockToken = semaphore.attemptToAcquireLock(
					this.lockKey, this.lockTimeoutSec, this.maxLockCount);
			// start with a new callback.
			ProgressCallback progressCallback = new SynchronizedProgressCallback(this.lockTimeoutSec);
			// listen to progress events
			ProgressListener listener = new ProgressListener() {

				@Override
				public void progressMade() {
					if(!canRun()){ //TODO: test
						throw new IllegalStateException("Gate closed");
					}
					// Give the lock more time
					semaphore.refreshLockTimeout(lockKey,
							lockToken, lockTimeoutSec);
				}
			};
			progressCallback.addProgressListener(listener);

			// Only proceed if a lock was acquired
			if (lockToken != null) {
				try {
					// Let the runner go while holding the lock
					runner.run(progressCallback);
				} finally {
					progressCallback.removeProgressListener(listener);
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


	boolean canRun(){
		return this.gate == null || this.gate.canRun();
	}
}
