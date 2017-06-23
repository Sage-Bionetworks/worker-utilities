package org.sagebionetworks.workers.util.aws.message;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.sagebionetworks.common.util.progress.ProgressCallback;

import com.amazonaws.services.sqs.model.Message;

/**
 * An implementation of a MessageDrivenRunner that wraps an actual
 * MessageDrivenRunner. This runner will continuously generate progress events,
 * at the provided frequency, as long as the wrapped runner is executing. This
 * is achieved by executing the wrapped runner on a separate thread that is
 * monitored from the original calling thread. Therefore, progress events will
 * originate from the calling thread, while the actual runner works on a
 * separate thread.
 * 
 */
public class AutoProgressingMessageDrivenRunner implements MessageDrivenRunner {

	private MessageDrivenRunner toWrap;
	ExecutorService executor;
	long progressFrequencyMS;
	
	

	/**
	 * @param toWrap The actual runner.
	 * @param progressFrequencyMs The frequency of progress events in MS.
	 */
	public AutoProgressingMessageDrivenRunner(MessageDrivenRunner toWrap,
			long progressFrequencyMs) {
		this(Executors.newFixedThreadPool(1), toWrap, progressFrequencyMs);
	}
	
	/**
	 * 
	 * @param executor 
	 * @param toWrap
	 * @param progressFrequencyMs
	 */
	public AutoProgressingMessageDrivenRunner(ExecutorService executor, MessageDrivenRunner toWrap,
			long progressFrequencyMs) {
		super();
		this.toWrap = toWrap;
		this.progressFrequencyMS = progressFrequencyMs;
		this.executor = executor;
	}


	@Override
	public void run(final ProgressCallback<Void> progressCallback,
			final Message message) throws RecoverableMessageException,
			Exception {
		// start the process
		Future<Void> future = executor.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				toWrap.run(progressCallback, message);
				return null;
			}
		});
		// make progress at least once.
		progressCallback.progressMade(null);
		while (true) {
			// wait for the process to finish
			try {
				future.get(progressFrequencyMS, TimeUnit.MILLISECONDS);
				// done.
				return;
			} catch (ExecutionException e) {
				// concert to the cause if we can.
				Throwable cause = e.getCause();
				if (cause instanceof Exception) {
					throw ((Exception) cause);
				}
				throw e;
			} catch (TimeoutException e) {
				// make progress for each timeout
				progressCallback.progressMade(null);
			}
		}
	}

}
