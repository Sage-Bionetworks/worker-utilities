package org.sagebionetworks.workers.util.semaphore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressListener;
import org.sagebionetworks.database.semaphore.CountingSemaphore;

public class ReadLockBundle implements AutoCloseable {
	
	private static final Logger log = LogManager
			.getLogger(ReadLockBundle.class);

	private final ProgressCallback callback;
	private final CountingSemaphore countingSemaphore;
	private final int maxNumberOfReaders;
	private final String[] lockKeys;
	private final Map<String, String> keyToTokenMap;
	private ProgressListener listener;


	public ReadLockBundle(ProgressCallback callback, CountingSemaphore countingSemaphore, int maxNumberOfReader, String... lockKeys) {
		super();
		if (callback == null) {
			throw new IllegalArgumentException("ProgressCallback cannot be null");
		}
		if(countingSemaphore == null) {
			throw new IllegalArgumentException("CountingSemaphore cannot be null");
		}
		if (lockKeys == null) {
			throw new IllegalArgumentException("LockKeys cannot be null");
		}
		if (lockKeys.length < 1) {
			throw new IllegalArgumentException("Must include at least one lock key");
		}
		for(String lockKey: lockKeys) {
			if(lockKey == null) {
				throw new IllegalArgumentException("Lock key cannot be null");
			}
		}
		this.keyToTokenMap = new HashMap<>(lockKeys.length);
		this.callback = callback;
		this.countingSemaphore = countingSemaphore;
		this.maxNumberOfReaders = maxNumberOfReader;
		this.lockKeys = lockKeys;
	}
	
	/**
	 * Attempt to acquire all locks.
	 */
	public void acquireAllReadLocks() {
		// Stop if there are any outstanding write locks.
		for (String lockKey : lockKeys) {
			String writeLockKey = Constants.createWriterLockKey(lockKey);
			if (countingSemaphore.existsUnexpiredLock(writeLockKey)) {
				throw new LockUnavilableException("Cannot get an read lock for key: " + lockKey);
			}
		}
		
		// acquire a read lock for each key
		for(String lockKey: lockKeys) {
			String readLockKey = Constants.createReaderLockKey(lockKey);
			String readToken = this.countingSemaphore.attemptToAcquireLock(readLockKey, callback.getLockTimeoutSeconds(), maxNumberOfReaders);
			if(readToken == null){
				throw new LockUnavilableException("Cannot get an read lock for key: "+lockKey);
			}
			keyToTokenMap.put(readLockKey, readToken);
		}
		
		// listen to callback events
		this.listener = () -> {
			Iterator<String> iterator = keyToTokenMap.keySet().iterator();
			while(iterator.hasNext()) {
				String readLockKey = iterator.next();
				String readToken = keyToTokenMap.get(readLockKey);
				countingSemaphore.refreshLockTimeout(readLockKey, readToken, callback.getLockTimeoutSeconds());
			}
		};
		callback.addProgressListener(listener);
	}

	@Override
	public void close() throws IOException {
		if(this.listener != null) {
			callback.removeProgressListener(this.listener);
		}
		Iterator<String> iterator = keyToTokenMap.keySet().iterator();
		Exception lastException = null;
		while(iterator.hasNext()) {
			// each lock must be released even if some of the lock release attempts fail.
			try {
				String readLockKey = iterator.next();
				String readToken = keyToTokenMap.get(readLockKey);
				countingSemaphore.releaseLock(readLockKey, readToken);
			} catch (Exception e) {
				lastException = e;
				log.error("Failed to release lock:", e);
			}
		}
		if(lastException != null) {
			throw new IOException(lastException);
		}
	}

}
