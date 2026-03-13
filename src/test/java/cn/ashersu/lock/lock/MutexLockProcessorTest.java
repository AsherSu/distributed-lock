package cn.ashersu.lock.lock;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.statemachine.LockResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class MutexLockProcessorTest {

    private MutexLockProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new MutexLockProcessor();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private LockCommand acquire(String key, String clientId, String threadId, long ttlMs) {
        return LockCommand.acquire(LockType.MUTEX, key, clientId, threadId, ttlMs, UUID.randomUUID().toString());
    }

    private LockCommand release(String key, String clientId, String threadId, long fencingToken) {
        return LockCommand.release(LockType.MUTEX, key, clientId, threadId, fencingToken);
    }

    private LockCommand renew(String key, String clientId, String threadId, long ttlMs) {
        return LockCommand.renew(LockType.MUTEX, key, clientId, threadId, ttlMs);
    }

    // -------------------------------------------------------------------------
    // Acquire tests
    // -------------------------------------------------------------------------

    @Test
    void acquire_whenFree_shouldSucceedWithPositiveFencingToken() {
        LockResult result = processor.process(acquire("lock:1", "c1", "t1", 5000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertTrue(result.getFencingToken() > 0);
    }

    @Test
    void acquire_whenHeldByOther_shouldReturnLocked() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));

        LockResult result = processor.process(acquire("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.LOCKED, result.getStatus());
        assertTrue(result.getRemainingTtlMs() > 0);
    }

    @Test
    void acquire_whenExpired_shouldGrantNewLock() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50));
        Thread.sleep(100);

        LockResult result = processor.process(acquire("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertTrue(result.getFencingToken() > 0);
    }

    @Test
    void fencingToken_shouldBeMonotonicallyIncreasing() {
        LockResult r1 = processor.process(acquire("lock:1", "c1", "t1", 5000));
        processor.process(release("lock:1", "c1", "t1", r1.getFencingToken()));

        LockResult r2 = processor.process(acquire("lock:1", "c2", "t2", 5000));

        assertTrue(r2.getFencingToken() > r1.getFencingToken());
    }

    // -------------------------------------------------------------------------
    // Release tests
    // -------------------------------------------------------------------------

    @Test
    void release_byOwnerWithCorrectToken_shouldSucceed() {
        LockResult acquired = processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(release("lock:1", "c1", "t1", acquired.getFencingToken()));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void release_byNonOwner_shouldReturnStale() {
        LockResult acquired = processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(release("lock:1", "c2", "t2", acquired.getFencingToken()));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    @Test
    void release_withWrongFencingToken_shouldReturnStale() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(release("lock:1", "c1", "t1", 9999L));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    @Test
    void release_whenExpired_shouldSucceed() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50));
        Thread.sleep(100);

        LockResult result = processor.process(release("lock:1", "c1", "t1", 1L));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void release_whenNeverAcquired_shouldSucceed() {
        LockResult result = processor.process(release("lock:absent", "c1", "t1", 1L));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    // -------------------------------------------------------------------------
    // Renew tests
    // -------------------------------------------------------------------------

    @Test
    void renew_byOwner_shouldSucceedAndExtendExpiry() {
        LockResult acquired = processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(renew("lock:1", "c1", "t1", 10000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertEquals(acquired.getFencingToken(), result.getFencingToken());
    }

    @Test
    void renew_byNonOwner_shouldReturnStale() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(renew("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    @Test
    void renew_whenExpired_shouldReturnStale() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50));
        Thread.sleep(100);

        LockResult result = processor.process(renew("lock:1", "c1", "t1", 5000));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    @Test
    void renew_whenNeverAcquired_shouldReturnStale() {
        LockResult result = processor.process(renew("lock:absent", "c1", "t1", 5000));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    // -------------------------------------------------------------------------
    // Performance tests
    // -------------------------------------------------------------------------

    @Test
    void perf_singleThread_acquireReleaseThroughput() {
        final int iterations = 100_000;
        final String key = "perf:lock";

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            LockResult acq = processor.process(acquire(key, "c1", "t1", 60_000));
            processor.process(release(key, "c1", "t1", acq.getFencingToken()));
        }
        long elapsed = System.nanoTime() - start;

        double opsPerSec = iterations / (elapsed / 1_000_000_000.0);
        System.out.printf("[MutexLock] Single-thread throughput: %.0f ops/s%n", opsPerSec);
    }

    @Test
    void perf_multiThread_concurrentAcquireRelease() throws InterruptedException {
        final int threads = 8;
        final int iterationsPerThread = 10_000;
        final String key = "perf:concurrent";
        final AtomicLong totalOps = new AtomicLong(0);
        final CountDownLatch ready = new CountDownLatch(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            final String clientId = "c" + i;
            new Thread(() -> {
                ready.countDown();
                try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                long ops = 0;
                for (int j = 0; j < iterationsPerThread; j++) {
                    LockResult acq = processor.process(acquire(key, clientId, "t0", 60_000));
                    if (acq.isSuccess()) {
                        processor.process(release(key, clientId, "t0", acq.getFencingToken()));
                        ops++;
                    }
                }
                totalOps.addAndGet(ops);
                done.countDown();
            }).start();
        }

        ready.await();
        long startNs = System.nanoTime();
        start.countDown();
        done.await();
        long elapsed = System.nanoTime() - startNs;

        double opsPerSec = totalOps.get() / (elapsed / 1_000_000_000.0);
        System.out.printf("[MutexLock] Multi-thread (%d threads) throughput: %.0f successful acquire+release ops/s%n",
                threads, opsPerSec);
    }
}
