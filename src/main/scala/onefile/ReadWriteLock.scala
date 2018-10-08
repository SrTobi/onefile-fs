package onefile

import java.util.concurrent.locks.ReentrantReadWriteLock

private class ReadWriteLock {
    private val (readLock, writeLock) = {
        val lock = new ReentrantReadWriteLock
        (lock.readLock(), lock.writeLock())
    }

    def read[T](body: => T): T = {
        try {
            readLock.lock()
            body
        } finally {
            readLock.unlock()
        }
    }

    def write[T](body: => T): T = {
        try {
            writeLock.lock()
            body
        } finally {
            writeLock.unlock()
        }
    }
}
