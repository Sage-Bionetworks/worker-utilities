package org.sagebionetworks.workers.util.semaphore;

import org.sagebionetworks.database.semaphore.CountingSemaphore;
import org.sagebionetworks.workers.util.Gate;
import org.sagebionetworks.workers.util.GatedRunner;

/**
 * A semaphore gated worker stack that consists of two layers:
 * <ol>
 * <li>SemaphoreGatedRunner - This gate is used to control the total number of
 * workers of this type that can run across a cluster of worker machines. See {@link SemaphoreGatedRunner}.</li>
 * <li>Gate - When a gate is provided in the configuration, a GatedRunner will
 * be as the root of the stack, such that the stack will only run when the
 * {@link Gate#canRun()} returns true.</li>
 * </ol>
 * 
 */
public class SemaphoreGatedWorkerStack implements Runnable {

	private Runnable runner;

	public SemaphoreGatedWorkerStack(CountingSemaphore semaphore,
			SemaphoreGatedWorkerStackConfiguration config) {

		SemaphoreGatedRunnerImpl semaphoreGatedRunner = new SemaphoreGatedRunnerImpl(
				semaphore, config.getSemaphoreGatedRunnerConfig());
		if (config.getGate() != null) {
			runner = new GatedRunner(config.getGate(), semaphoreGatedRunner);
		} else {
			runner = semaphoreGatedRunner;
		}
	}

	@Override
	public void run() {
		runner.run();
	}

}
