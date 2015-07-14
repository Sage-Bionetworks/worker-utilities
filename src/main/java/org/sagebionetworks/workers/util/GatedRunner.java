package org.sagebionetworks.workers.util;

/**
 * A simple runner controlled with a gate.
 * 
 * When {@link GatedRunner#run()} is called the wrapped runner will be
 * {@link Runnable#run()} if the provide {@link Gate#canRun()} returns true.
 *
 */
public class GatedRunner implements Runnable {

	Gate gate;
	Runnable runner;

	public GatedRunner(Gate gate, Runnable runner) {
		super();
		if (gate == null) {
			throw new IllegalArgumentException("Gate cannot be null");
		}
		if (runner == null) {
			throw new IllegalArgumentException("Runner cannot be null");
		}
		this.gate = gate;
		this.runner = runner;
	}

	@Override
	public void run() {
		if (gate.canRun()) {
			try {
				runner.run();
			} catch (Exception e) {
				gate.runFailed(e);
			}
		}
	}

}
