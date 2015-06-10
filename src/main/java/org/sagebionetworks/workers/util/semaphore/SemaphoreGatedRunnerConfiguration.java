package org.sagebionetworks.workers.util.semaphore;

import org.sagebionetworks.workers.util.progress.ProgressingRunner;

public class SemaphoreGatedRunnerConfiguration<T> {
	
	ProgressingRunner<T> runner;
	String lockKey;
	long lockTimeoutSec = -1;
	int maxLockCount = -1;

	public SemaphoreGatedRunnerConfiguration() {
	}

	public SemaphoreGatedRunnerConfiguration(ProgressingRunner<T> runner,
			String lockKey, long lockTimeoutSec, int maxLockCount) {
		super();
		this.runner = runner;
		this.lockKey = lockKey;
		this.lockTimeoutSec = lockTimeoutSec;
		this.maxLockCount = maxLockCount;
	}

	public ProgressingRunner<T> getRunner() {
		return runner;
	}

	public void setRunner(ProgressingRunner<T> runner) {
		this.runner = runner;
	}

	public String getLockKey() {
		return lockKey;
	}

	public void setLockKey(String lockKey) {
		this.lockKey = lockKey;
	}

	public long getLockTimeoutSec() {
		return lockTimeoutSec;
	}

	public void setLockTimeoutSec(long lockTimeoutSec) {
		this.lockTimeoutSec = lockTimeoutSec;
	}

	public int getMaxLockCount() {
		return maxLockCount;
	}

	public void setMaxLockCount(int maxLockCount) {
		this.maxLockCount = maxLockCount;
	}	

}
