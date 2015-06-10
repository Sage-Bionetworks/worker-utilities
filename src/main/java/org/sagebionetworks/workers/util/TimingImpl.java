package org.sagebionetworks.workers.util;

public class TimingImpl implements Timing {

	@Override
	public long currentTimeMillis(){
		return System.currentTimeMillis();
	}
	
	@Override
	public void sleep(long millis) throws InterruptedException{
		Thread.sleep(millis);
	}
}
