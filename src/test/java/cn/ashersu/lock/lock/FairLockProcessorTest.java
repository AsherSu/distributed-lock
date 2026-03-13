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

    /**
     * 场景：无持锁方且等待队列为空，新请求应绕过队列直接授锁。
     * 验证无竞争时的快路径，避免不必要的入队开销。
     */
    @Test
    void acquire_whenFreeAndNoQueue_shouldGrantLockDirectly() {
        LockResult result = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertTrue(result.getFencingToken() > 0);
    }

    /**
     * 场景：锁被 c1 占用时 c2 到来，c2 入队等待并收到 LOCKED 响应。
     * 公平锁以 FIFO 队列管理等待者，不允许抢占，保证请求不饥饿。
     */
    @Test
    void acquire_whenHeldByOther_shouldEnqueueAndReturnLocked() {
        processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        LockResult result = processor.process(acquire("lock:1", "c2", "t2", 5000, 3000));

        assertEquals(LockResult.Status.LOCKED, result.getStatus());
    }

    /**
     * 场景：持锁方 TTL 到期，队列为空，下一个请求者触发懒清理后直接获锁。
     * 验证过期路径不遗留"死锁"状态。
     */
    @Test
    void acquire_whenExpired_shouldGrantNewLock() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50, 3000));
        Thread.sleep(100);

        LockResult result = processor.process(acquire("lock:1", "c2", "t2", 5000, 3000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    /**
     * 场景：持锁方网络重试时幂等加锁，应直接返回 SUCCESS 并复用原 fencing token。
     * 重试不能触发新锁授权（token 不得推进），否则下游会误判持锁方发生了变更。
     */
    @Test
    void acquire_idempotent_sameOwnerAlreadyHolding_shouldReturnSuccess() {
        LockResult first = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));
        LockResult second = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        assertEquals(LockResult.Status.SUCCESS, first.getStatus());
        assertEquals(LockResult.Status.SUCCESS, second.getStatus());
        assertEquals(first.getFencingToken(), second.getFencingToken());
    }

    /**
     * 场景：等待中的 c2 因网络重试再次发送 ACQUIRE，不应被二次入队。
     * 队列重复条目会导致 c2 实际持锁后仍有"幽灵副本"阻塞后续等待者。
     * 此场景在 Raft 日志重放时同样需要幂等保障。
     */
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

    /**
     * 场景：公平性核心验证——锁空闲但等待队列仍非空时，新到请求者不得插队。
     * c1 释放后 c2（队首）晋升为持锁方，此时 c3 到来仍须排到队尾，不能越过 c2。
     */
    @Test
    void acquire_fairness_newArrivalShouldQueueBehindExistingWaiters() {
        // c1 holds the lock
        processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));
        // c2 queues
        processor.process(acquire("lock:1", "c2", "t2", 5000, 3000));

        // Release c1: c2 gets promoted to lock holder
        LockResult acquired = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));
        processor.process(release("lock:1", "c1", "t1", acquired.getFencingToken()));

        // c3 arrives after c2 has been promoted — must still queue behind c2
        LockResult c3 = processor.process(acquire("lock:1", "c3", "t3", 5000, 3000));
        assertEquals(LockResult.Status.LOCKED, c3.getStatus());
    }

    // -------------------------------------------------------------------------
    // Release tests
    // -------------------------------------------------------------------------

    /**
     * 场景：持锁方 c1 释放后，队首等待者 c2 自动晋升为新持锁方。
     * c3 此后到来应看到锁已被 c2 持有而被阻塞，验证队列推进的完整性。
     */
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

    /**
     * 场景：等待中的 c2 主动取消等待（发送 RELEASE），应从队列中移除并返回 SUCCESS。
     * 取消等待不影响 c1 的持锁状态，c3 此时仍被 c1 阻塞。
     */
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

    /**
     * 场景：c2 既不是持锁方也不在等待队列中，尝试释放锁。
     * 此类无效操作应返回 STALE，防止误操作影响锁状态。
     */
    @Test
    void release_byNonOwnerNotInQueue_shouldReturnStale() {
        LockResult c1Acq = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        LockResult result = processor.process(release("lock:1", "c2", "t2", c1Acq.getFencingToken()));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    /**
     * 场景：owner 携带错误的 fencing token 尝试释放锁。
     * 防止 STW/Full-GC 后过期客户端实例持旧 token 错误释放当前有效锁。
     */
    @Test
    void release_withWrongFencingToken_shouldReturnStale() {
        processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        LockResult result = processor.process(release("lock:1", "c1", "t1", 9999L));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    /**
     * 场景：持锁方 TTL 过期后才收到 RELEASE，过期路径同样须触发队列推进。
     * 验证"过期释放"与"正常释放"在晋升行为上保持一致。
     */
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

    /**
     * 场景：watchdog 正常续期，仅延长 TTL，fencing token 保持不变。
     * 公平锁的持锁方续期语义与互斥锁相同，续期不触发重新排队或 token 推进。
     */
    @Test
    void renew_byOwner_shouldSucceed() {
        LockResult acq = processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));
        LockResult result = processor.process(renew("lock:1", "c1", "t1", 10000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertEquals(acq.getFencingToken(), result.getFencingToken());
    }

    /**
     * 场景：非持锁方尝试续期（如误操作或恶意延长他人锁时间），应被拒绝。
     */
    @Test
    void renew_byNonOwner_shouldReturnStale() {
        processor.process(acquire("lock:1", "c1", "t1", 5000, 3000));

        LockResult result = processor.process(renew("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    /**
     * 场景：锁过期后 watchdog 才发来续期请求（续期与过期的竞态），应返回 STALE。
     * 防止"僵尸续期"覆盖已由队首等待者晋升持有的新锁有效期。
     */
    @Test
    void renew_whenExpired_shouldReturnStale() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50, 3000));
        Thread.sleep(100);

        LockResult result = processor.process(renew("lock:1", "c1", "t1", 5000));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    /**
     * 场景：对从未加锁的 key 执行续期，等同于锁已过期，返回 STALE。
     */
    @Test
    void renew_whenNeverAcquired_shouldReturnStale() {
        LockResult result = processor.process(renew("lock:absent", "c1", "t1", 5000));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    // -------------------------------------------------------------------------
    // Performance tests
    // -------------------------------------------------------------------------

    /**
     * 场景：单线程无竞争 acquire+release，排除队列管理开销外的变量。
     * 相比 MutexLock 基准，差值即为公平锁队列检查的额外成本。
     */
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

    /**
     * 场景：8 线程竞争同一把公平锁，量化 FIFO 队列管理在高竞争下的整体吞吐。
     * 与 MutexLock 多线程基准对比，可评估公平性带来的额外开销。
     */
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

    /**
     * 场景：预填深度为 1000 的等待队列，然后逐个释放触发晋升，测量队列推进的连续吞吐。
     * 此指标直接反映 promoteNextWaiter 在大队列下的性能，是公平锁独有的开销来源。
     */
    @Test
    void perf_queuePromotion_chainedAcquireRelease() {
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
