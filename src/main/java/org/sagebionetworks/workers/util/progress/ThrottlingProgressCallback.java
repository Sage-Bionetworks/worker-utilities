package org.sagebionetworks.workers.util.progress;


/**
 * This implementation of a ProgressCallback wraps a target ProgressCallback and
 * acts as a throttle between a frequently firing workers and the target
 * callback. The first call made to {@link #progressMade(Object)} will be
 * forwarded to the target, while all subsequent calls will only be forwarded to
 * the target if the configured frequency time in MS has elapsed since the last
 * forwarded call.
 * 
 * This throttle allows a worker to call {@link #progressMade(Object)} as frequently as
 * possible without overwhelming the target.
 * 
 * @param <T>
 */
public class ThrottlingProgressCallback<T> implements ProgressCallback<T> {

	ProgressCallback<T> targetCallback;
	long frequencyMS;
	long lastFiredTime;

	/**
	 * @param targetCallback Calls to {@link #progressMade(Object)} will be forward to this target unless throttled.
	 * @param frequencyMS The frequency in milliseconds that calls should be forwarded to the target.
	 */
	public ThrottlingProgressCallback(ProgressCallback<T> targetCallback, long frequencyMS) {
		super();
		this.targetCallback = targetCallback;
		this.frequencyMS = frequencyMS;
		this.lastFiredTime = -1;
	}

	@Override
	public void progressMade(T t) {
		long now = System.currentTimeMillis();
		if (this.lastFiredTime < 0) {
			// first call is forwarded.
			this.lastFiredTime = now;
			this.targetCallback.progressMade(t);
		} else {
			if (now - this.lastFiredTime > frequencyMS) {
				// first call is forwarded.
				this.lastFiredTime = now;
				this.targetCallback.progressMade(t);
			}
		}
	}

}
