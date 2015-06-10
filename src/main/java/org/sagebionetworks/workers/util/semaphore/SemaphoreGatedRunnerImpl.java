package org.sagebionetworks.workers.util.semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.database.semaphore.CountingSemaphore;
import org.sagebionetworks.workers.util.progress.ProgressCallback;
import org.sagebionetworks.workers.util.progress.ProgressingRunner;
import org.sagebionetworks.workers.util.progress.ThrottlingProgressCallback;

/**
 * This is not a singleton. A new instance of this gate must be created each time you need one.
 * @param <T>
 *
 */
public class SemaphoreGatedRunnerImpl<T> implements SemaphoreGatedRunner {
	
	private static final Logger log = LogManager.getLogger(SemaphoreGatedRunnerImpl.class);

	CountingSemaphore semaphore;
	ProgressingRunner<T> runner;
	String lockKey;
	long lockTimeoutSec = -1;
	int maxLockCount = -1;
	
	/**
	 * IoC constructor.
	 * @param semaphore
	 */
	public SemaphoreGatedRunnerImpl(CountingSemaphore semaphore) {
		super();
		this.semaphore = semaphore;
	}

	/**
	 * Must be configured before calling {@link #run()}
	 * 
	 * @param config
	 */
	public void setConfiguration(SemaphoreGatedRunnerConfiguration<T> config){
		if(config == null){
			throw new IllegalArgumentException("Configuration cannot be null");
		}
		this.runner = config.getRunner();
		this.lockKey = config.lockKey;
		this.lockTimeoutSec = config.getLockTimeoutSec();
		this.maxLockCount = config.getMaxLockCount();
		validateConfig();
	}
	
	/**
	 * This is the run of the 'runnable'
	 */
	public void run() {
		validateConfig();
		try {
			// attempt to get a lock
			final String lockToken = semaphore.attemptToAcquireLock(this.lockKey, this.lockTimeoutSec, this.maxLockCount);
			// the frequency that {@link ProgressCallback#progressMade(Object)} calls can refresh the lock in the DB.
			long throttleFrequencyMS = (this.lockTimeoutSec*1000)/3;
			// Only proceed if a lock was acquired
			if(lockToken != null){
				try{
					// Let the runner go while holding the lock
					runner.run(new ThrottlingProgressCallback<T>(new ProgressCallback<T>() {
						@Override
						public void progressMade(T t) {
							// Give the lock more time
							semaphore.refreshLockTimeout(lockKey, lockToken, lockTimeoutSec);
						}
					}, throttleFrequencyMS));
				}finally{
					semaphore.releaseLock(this.lockKey, lockToken);
				}
			}
		} catch (Throwable e) {
			log.error(e);
		}
	}
	
	private void validateConfig(){
		if(this.runner == null){
			throw new IllegalArgumentException("Runner cannot be be null");
		}
		if(this.lockKey == null){
			throw new IllegalArgumentException("Lock key cannot be null");
		}
		if(lockTimeoutSec < 1){
			throw new IllegalArgumentException("LockTimeoutSec cannot be less than one.");
		}
		if(maxLockCount < 1){
			throw new IllegalArgumentException("MaxLockCount cannot be less than one.");
		}
	}

}
