package org.sagebionetworks.workers.util.progress;

public interface ProgressingRunner<T> {
	
	public void run(ProgressCallback<T> progressCallback);

}
