package org.sagebionetworks.workers.util;

/**
 * Simple implementation of a Cock.
 *
 */
public class ClockImpl implements Clock {

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.workers.util.Clock#sleep(long)
	 */
	@Override
	public void sleep(long sleepMs) throws InterruptedException {
		Thread.sleep(sleepMs);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.workers.util.Clock#currentTimeMillis()
	 */
	@Override
	public long currentTimeMillis() {
		return System.currentTimeMillis();
	}

}
