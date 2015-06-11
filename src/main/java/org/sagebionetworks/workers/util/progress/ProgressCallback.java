package org.sagebionetworks.workers.util.progress;

/**
 * A callback for workers that need to notify containers that progress is being made.
 *
 * @param <T> The parameter type passed to the
 *            {@link ProgressCallback#progressMade(Object)}.
 */
public interface ProgressCallback<T> {

	/**
	 * Called when a worker makes progress.
	 * @param t
	 */
	public void progressMade(T t);
}
