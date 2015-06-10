package org.sagebionetworks.workers.util;

/**
 * As simple abstraction over the System time and thread sleep.
 *
 */
public interface Timing {

	/**
	 * Same as {@link java.lang.System#currentTimeMillis()}
	 * @param millis
	 * @throws InterruptedException
	 */
	void sleep(long millis) throws InterruptedException;

	/**
	 * Same as {@link java.lang.Thread#sleep(long)}
	 * @return
	 */
	long currentTimeMillis();
	
}
