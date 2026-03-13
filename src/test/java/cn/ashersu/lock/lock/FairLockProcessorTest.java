package cn.ashersu.lock.lock;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.statemachine.LockResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class FairLockProcessorTest {

    private FairLockProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new FairLockProcessor();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private LockCommand acquire(String key, String clientId, String threadId, long ttlMs, long waitMs) {
        LockCommand cmd = LockCommand.acquire(LockType.FAIR, key, clientId, threadId, ttlMs, UUID.randomUUID().toString());
        cmd.setWaitTime(waitMs);
        return cmd;
    }

    private LockCommand release(String key, String clientId, String threadId, long fencingToken) {
        return LockCommand.release(LockType.FAIR, key, clientId, threadId, fencingToken);
    }

    private LockCommand renew(String key, String clientId, String threadId, long ttlMs) {
        return LockCommand.renew(LockType.FAIR, key, clientId, threadId, ttlMs);
    }

    // -------------------------------------------------------------------------
    // Acquire tests
    // -------------------------------------------------------------------------

    @Test
    void acquire_whenFreeAndNoQueue_shouldGrantLockDirectly() {
        LockResult result = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertTrue(result.getFencingToken() > 0);
    }

    @Test
    void acquire_whenHeldByOther_shouldEnqueueAndReturnLocked() {
        processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        LockResult result = processor.process(acquire("lock:1", "c2", "t2", 5000, 3000));

        assertEquals(LockResult.Status.LOCKED, result.getStatus());
    }

    @Test
    void acquire_whenExpired_shouldGrantNewLock() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50, 3000));
        Thread.sleep(100);

        LockResult result = processor.process(acquire("lock:1", "c2", "t2", 5000, 3000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void acquire_idempotent_sameOwnerAlreadyHolding_shouldReturnSuccess() {
        LockResult first = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));
        LockResult second = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        assertEquals(LockResult.Status.SUCCESS, first.getStatus());
        assertEquals(LockResult.Status.SUCCESS, second.getStatus());
        assertEquals(first.getFencingToken(), second.getFencingToken());
    }

    @Test
    void acquire_idempotent_sameWaiterAlreadyQueued_shouldNotEnqueueTwice() {
        // c1 holds the lock
        processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));
        // c2 joins the queue
        LockResult first = processor.process(acquire("lock:1", "c2", "t2", 5000, 3000));
        // c2 requests again — already queued, should not double-enqueue
        LockResult second = processor.process(acquire("lock:1", "c2", "t2", 5000, 3000));

        assertEquals(LockResult.Status.LOCKED, first.getStatus());
        assertEquals(LockResult.Status.LOCKED, second.getStatus());
    }

    @Test
    void acquire_fairness_newArrivalShouldQueueBehindExistingWaiters() {
        // c1 holds the lock
        processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));
        // c2 queues
        processor.process(acquire("lock:1", "c2", "t2", 5000, 3000));

        // c3 arrives while lock is free of holders but queue still has c2 — must queue
        // Release c1 first so the lock becomes free but queue still has c2
        LockResult acquired = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));
        // c1 is already holder; release it
        processor.process(release("lock:1", "c1", "t1", acquired.getFencingToken()));

        // Now c2 should be the new holder. c3 comes in — lock is "held by c2" after promotion.
        // Let's verify c3 cannot cut in ahead of c2 by checking c3 gets LOCKED
        LockResult c3 = processor.process(acquire("lock:1", "c3", "t3", 5000, 3000));
        assertEquals(LockResult.Status.LOCKED, c3.getStatus());
    }

    // -------------------------------------------------------------------------
    // Release tests
    // -------------------------------------------------------------------------

    @Test
    void release_byOwner_shouldSucceedAndPromoteWaiter() {
        // c1 holds, c2 waits
        LockResult c1Acq = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));
        processor.process(acquire("lock:1", "c2", "t2", 5000, 3000));

        LockResult releaseResult = processor.process(release("lock:1", "c1", "t1", c1Acq.getFencingToken()));
        assertEquals(LockResult.Status.SUCCESS, releaseResult.getStatus());

        // c2 should now hold the lock — c3 should get LOCKED
        LockResult c3 = processor.process(acquire("lock:1", "c3", "t3", 5000, 3000));
        assertEquals(LockResult.Status.LOCKED, c3.getStatus());
    }

    @Test
    void release_byNonOwnerInQueue_shouldRemoveFromQueueAndSucceed() {
        // c1 holds, c2 queues
        LockResult c1Acq = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));
        processor.process(acquire("lock:1", "c2", "t2", 5000, 3000));

        // c2 cancels its wait
        LockResult cancel = processor.process(release("lock:1", "c2", "t2", 0L));
        assertEquals(LockResult.Status.SUCCESS, cancel.getStatus());

        // c1 still holds the lock
        LockResult c3 = processor.process(acquire("lock:1", "c3", "t3", 5000, 3000));
        assertEquals(LockResult.Status.LOCKED, c3.getStatus());
    }

    @Test
    void release_byNonOwnerNotInQueue_shouldReturnStale() {
        LockResult c1Acq = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        LockResult result = processor.process(release("lock:1", "c2", "t2", c1Acq.getFencingToken()));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    @Test
    void release_withWrongFencingToken_shouldReturnStale() {
        processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        LockResult result = processor.process(release("lock:1", "c1", "t1", 9999L));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    @Test
    void release_whenExpired_shouldSucceedAndPromoteQueue() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50, 3000));
        processor.process(acquire("lock:1", "c2", "t2", 5000, 3000));
        Thread.sleep(100);

        LockResult result = processor.process(release("lock:1", "c1", "t1", 1L));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    // -------------------------------------------------------------------------
    // Renew tests
    // -------------------------------------------------------------------------

    @Test
    void renew_byOwner_shouldSucceed() {
        LockResult acq = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));
        LockResult result = processor.process(renew("lock:1", "c1", "t1", 10000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertEquals(acq.getFencingToken(), result.getFencingToken());
    }

    @Test
    void renew_byNonOwner_shouldReturnStale() {
        processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        LockResult result = processor.process(renew("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    @Test
    void renew_whenExpired_shouldReturnStale() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50, 3000));
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
            LockResult acq = processor.process(acquire(key, "c1", "t1", 60_000, 0));
            processor.process(release(key, "c1", "t1", acq.getFencingToken()));
        }
        long elapsed = System.nanoTime() - start;

        double opsPerSec = iterations / (elapsed / 1_000_000_000.0);
        System.out.printf("[FairLock] Single-thread throughput: %.0f ops/s%n", opsPerSec);
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
                    LockResult acq = processor.process(acquire(key, clientId, "t0", 60_000, 0));
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
        System.out.printf("[FairLock] Multi-thread (%d threads) throughput: %.0f successful acquire+release ops/s%n",
                threads, opsPerSec);
    }

    @Test
    void perf_queuePromotion_chainedAcquireRelease() {
        // Pre-fill a queue of N clients then release them one by one, measuring promotion latency
        final int queueDepth = 1000;
        final String key = "perf:queue";
        final long ttl = 60_000;

        // c0 grabs the lock first
        LockResult holder = processor.process(acquire(key, "c0", "t0", ttl, ttl));

        // Enqueue queueDepth waiters
        for (int i = 1; i <= queueDepth; i++) {
            processor.process(acquire(key, "c" + i, "t0", ttl, ttl));
        }

        // Now release in chain, each release promotes the next waiter
        long start = System.nanoTime();
        LockResult current = holder;
        for (int i = 0; i <= queueDepth; i++) {
            processor.process(release(key, "c" + i, "t0", current.getFencingToken()));
            // The next holder gets a new fencing token after promotion
            current = processor.process(acquire(key, "c" + (i + 1), "t0", ttl, 0));
        }
        long elapsed = System.nanoTime() - start;

        double promotionsPerSec = (queueDepth + 1) / (elapsed / 1_000_000_000.0);
        System.out.printf("[FairLock] Queue promotion throughput: %.0f promotions/s (depth=%d)%n",
                promotionsPerSec, queueDepth);
    }
}
