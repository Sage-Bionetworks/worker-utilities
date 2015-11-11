package org.sagebionetworks.workers.util.progress;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import org.mockito.Mockito;
import org.sagebionetworks.common.util.progress.ProgressCallback;

public class ThrottlingProgressCallbackTest {
	
	ProgressCallback<String> mockTarget;
	
	@SuppressWarnings("unchecked")
	@Before
	public void before(){
		mockTarget = Mockito.mock(ProgressCallback.class);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testThrottle() throws InterruptedException{
		long frequency = 1000;
		ThrottlingProgressCallback<String> throttle = new ThrottlingProgressCallback<String>(mockTarget, frequency);
		// the first call should get forwarded
		throttle.progressMade("foo");
		verify(mockTarget).progressMade("foo");
		reset(mockTarget);
		// Next call should not go through
		throttle.progressMade("foo");
		verify(mockTarget, never()).progressMade(anyString());
		// wait for enough to to pass.
		Thread.sleep(frequency+1);
		throttle.progressMade("foo");
		verify(mockTarget).progressMade("foo");
		reset(mockTarget);
		// Next call should not go through
		throttle.progressMade("foo");
		verify(mockTarget, never()).progressMade(anyString());
	}

}
