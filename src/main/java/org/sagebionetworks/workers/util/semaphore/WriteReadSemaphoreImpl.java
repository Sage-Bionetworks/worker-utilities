package org.sagebionetworks.workers.util.semaphore;

import org.sagebionetworks.database.semaphore.CountingSemaphore;

public class WriteReadSemaphoreImpl implements WriteReadSemaphore {

	final CountingSemaphore countingSemaphore;
	final int maxNumberOfReaders;


	public WriteReadSemaphoreImpl(CountingSemaphore countingSemaphore, int maxNumberOfReaders) {
		if(countingSemaphore == null) {
			throw new IllegalArgumentException("CountingSemaphore cannot be null");
		}
		this.countingSemaphore = countingSemaphore;
		this.maxNumberOfReaders = maxNumberOfReaders;
	}
	
	@Override
	public WriteLock getWriteLock(WriteLockRequest request) throws Exception {
		if(request == null) {
			throw new IllegalArgumentException("Request cannot be null");
		}
		WriteLockImpl lock = new WriteLockImpl(countingSemaphore, request);
		try {
			lock.attemptToAcquireLock();
			return lock;
		} catch (Exception e) {
			lock.close();
			throw e;
		}
	}


	@Override
	public ReadLock getReadLock(ReadLockRequest request) throws Exception {
		if(request == null) {
			throw new IllegalArgumentException("Request cannot be null");
		}
		ReadLockImpl lock =  new ReadLockImpl(countingSemaphore, maxNumberOfReaders, request);
		try {
			lock.attemptToAcquireLock();
			return lock;
		} catch (Exception e) {
			lock.close();
			throw e;
		}
	}
}
