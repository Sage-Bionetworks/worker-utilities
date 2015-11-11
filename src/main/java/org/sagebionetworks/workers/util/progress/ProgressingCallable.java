package org.sagebionetworks.workers.util.progress;

import java.util.concurrent.Callable;

/**
 * Similar to {@link Callable} but the call method is provided with a
 * {@link ProgressCallback} that can be use to notify a container that progress
 * is still being made.
 * 
 * @param <R>
 *            The return type of the callable.
 *            {@link ProgressCallback#progressMade(Object)}.
 * @param <T>
 *            The parameter type passed to the
 *            {@link ProgressCallback#progressMade(Object)}.
 * */
public interface ProgressingCallable<R, T> {

	/**
	 * Similar to {@link Callable#call()} except a {@link ProgressCallback} is
	 * provided so the callable can notify a container that progress is still
	 * being made.
	 * 
	 * @param callback
	 * @return
	 * @throws Exception
	 */
	public R call(ProgressCallback<T> callback) throws Exception;

}
