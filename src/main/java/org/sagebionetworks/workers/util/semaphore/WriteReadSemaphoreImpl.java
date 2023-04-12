package org.sagebionetworks.workers.util.semaphore;

import java.util.Optional;

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
	public WriteLockProvider getWriteLockProvider(WriteLockRequest request) throws LockUnavilableException {
		if(request == null) {
			throw new IllegalArgumentException("Request cannot be null");
		}
		return new WriteLockProviderImpl(countingSemaphore, request);
	}


	@Override
	public ReadLockProvider getReadLockProvider(ReadLockRequest request) throws LockUnavilableException {
		if(request == null) {
			throw new IllegalArgumentException("Request cannot be null");
		}
		return new ReadLockProviderImpl(countingSemaphore, maxNumberOfReaders, request);
	}
}
