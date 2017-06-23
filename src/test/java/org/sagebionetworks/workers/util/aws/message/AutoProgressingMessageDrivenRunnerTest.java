package org.sagebionetworks.workers.util.aws.message;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.sagebionetworks.common.util.progress.ProgressCallback;

import com.amazonaws.services.sqs.model.Message;

public class AutoProgressingMessageDrivenRunnerTest {

	@Mock
	ExecutorService mockExecutor;

	@Mock
	Future<Void> mockFuture;

	@Mock
	MessageDrivenRunner mockRunners;
	@Mock
	ProgressCallback<Void> mockCallback;
	@Mock
	Message mockMessage;

	AutoProgressingMessageDrivenRunner auto;

	Integer returnValue;
	long progressFrequencyMs;

	@Before
	public void before() throws Exception {
		MockitoAnnotations.initMocks(this);
		returnValue = 101;

		progressFrequencyMs = 1000;
		doAnswer(new Answer<Future<Void>>(){

			@Override
			public Future<Void> answer(InvocationOnMock invocation)
					throws Throwable {
				Callable<Integer> callable = (Callable<Integer>) invocation.getArguments()[0];
				callable.call();
				return mockFuture;
			}}).when(mockExecutor).submit(any(Callable.class));
		// throw timeout twice then return a value.
		when(mockFuture.get(anyLong(), any(TimeUnit.class)))
				.thenThrow(new TimeoutException())
				.thenThrow(new TimeoutException()).thenReturn(null);

		auto = new AutoProgressingMessageDrivenRunner(mockExecutor,
				mockRunners, progressFrequencyMs);
	}

	@Test
	public void testCallHappy() throws Exception {
		// call under test.
		auto.run(mockCallback,  mockMessage);
		verify(mockExecutor).submit(any(Callable.class));
		verify(mockCallback, times(3)).progressMade(null);
		verify(mockRunners).run(mockCallback, mockMessage);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCallNonTimeoutException() throws Exception {
		reset(mockFuture);
		// future fails with non
		when(mockFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(
				new ExecutionException(new IllegalArgumentException("Unexpected")));
		// call under test.
		auto.run(mockCallback, mockMessage);
	}	

}
