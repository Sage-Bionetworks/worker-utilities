package org.sagebionetworks.workers.util;

/**
 * Abstraction for basic clock functions like sleep and current time.
 *
 */
public interface Clock {
	/**
	 * Sleep for the given number of milliseconds.
	 * @param sleepMs
	 * @throws InterruptedException 
	 */
	public void sleep(long sleepMs) throws InterruptedException;
	
	/**
	 * Get the current system time in MS.
	 * @return
	 */
	long currentTimeMillis();

}
