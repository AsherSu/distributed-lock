package cn.ashersu.lock.lock;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.statemachine.LockResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ReentrantLockProcessorTest {

    private ReentrantLockProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new ReentrantLockProcessor();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private LockCommand acquire(String key, String clientId, String threadId, long ttlMs) {
        return LockCommand.acquire(LockType.REENTRANT, key, clientId, threadId, ttlMs, UUID.randomUUID().toString());
    }

    private LockCommand release(String key, String clientId, String threadId, long fencingToken) {
        return LockCommand.release(LockType.REENTRANT, key, clientId, threadId, fencingToken);
    }

    private LockCommand renew(String key, String clientId, String threadId, long ttlMs) {
        return LockCommand.renew(LockType.REENTRANT, key, clientId, threadId, ttlMs);
    }

    // -------------------------------------------------------------------------
    // Basic acquire / release / renew (same as Mutex)
    // -------------------------------------------------------------------------

    /**
     * 场景：无竞争情况下首次加锁，验证基础授锁路径和 fencing token 初始值为正数。
     */
    @Test
    void acquire_whenFree_shouldSucceedWithPositiveFencingToken() {
        LockResult result = processor.process(acquire("lock:1", "c1", "t1", 5000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertTrue(result.getFencingToken() > 0);
    }

    /**
     * 场景：锁被 c1 持有期间 c2 尝试加锁，互斥语义不因"可重入"而消失。
     * 重入只对同一 owner 有效，不同 owner 仍遵守互斥规则，响应携带剩余 TTL。
     */
    @Test
    void acquire_whenHeldByOther_shouldReturnLocked() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));

        LockResult result = processor.process(acquire("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.LOCKED, result.getStatus());
        assertTrue(result.getRemainingTtlMs() >= 0);
    }

    /**
     * 场景：c1 的锁 TTL 到期后 c2 来 ACQUIRE，触发懒清理并授予新锁，验证过期路径正常工作。
     */
    @Test
    void acquire_whenExpired_shouldGrantNewLock() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50));
        Thread.sleep(100);

        LockResult result = processor.process(acquire("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    /**
     * 场景：同一 key 经历一次完整的 acquire-release 后再次加锁。
     * fencing token 必须严格递增，下游系统用它拒绝来自旧锁实例的操作。
     */
    @Test
    void fencingToken_shouldBeMonotonicallyIncreasing() {
        LockResult r1 = processor.process(acquire("lock:1", "c1", "t1", 5000));
        processor.process(release("lock:1", "c1", "t1", r1.getFencingToken()));

        LockResult r2 = processor.process(acquire("lock:1", "c2", "t2", 5000));

        assertTrue(r2.getFencingToken() > r1.getFencingToken());
    }

    /**
     * 场景：标准释放路径，owner + fencing token 双重校验均通过。
     */
    @Test
    void release_byOwner_shouldSucceed() {
        LockResult acquired = processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(release("lock:1", "c1", "t1", acquired.getFencingToken()));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    /**
     * 场景：非 owner 尝试释放他人的锁，应被拒绝，防止误操作。
     */
    @Test
    void release_byNonOwner_shouldReturnStale() {
        LockResult acquired = processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(release("lock:1", "c2", "t2", acquired.getFencingToken()));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    /**
     * 场景：owner 携带错误的 fencing token 尝试释放锁。
     * 防止 STW/Full-GC 后过期客户端实例持旧 token 错误释放当前有效锁。
     */
    @Test
    void release_withWrongFencingToken_shouldReturnStale() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(release("lock:1", "c1", "t1", 9999L));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    /**
     * 场景：锁已自然过期后 owner 仍发来 RELEASE（watchdog 续期失败等场景），幂等成功。
     */
    @Test
    void release_whenExpired_shouldSucceed() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50));
        Thread.sleep(100);

        LockResult result = processor.process(release("lock:1", "c1", "t1", 1L));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    /**
     * 场景：watchdog 正常续期，仅延长 TTL，fencing token 保持不变。
     * token 不变意味着续期不触发任何持锁方变更。
     */
    @Test
    void renew_byOwner_shouldSucceed() {
        LockResult acquired = processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(renew("lock:1", "c1", "t1", 10000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertEquals(acquired.getFencingToken(), result.getFencingToken());
    }

    /**
     * 场景：非 owner 尝试续期他人的锁，续期权限与持锁权等价，非 owner 应被拒绝。
     */
    @Test
    void renew_byNonOwner_shouldReturnStale() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(renew("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    /**
     * 场景：watchdog 续期与锁过期的竞态——续期请求到达时锁已过期。
     * 拒绝续期防止"僵尸续期"覆盖已被新 owner 持有的锁的有效期。
     */
    @Test
    void renew_whenExpired_shouldReturnStale() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50));
        Thread.sleep(100);

        LockResult result = processor.process(renew("lock:1", "c1", "t1", 5000));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    // -------------------------------------------------------------------------
    // Reentrant-specific tests
    // -------------------------------------------------------------------------

    /**
     * 场景：重入核心语义验证，同一 owner（clientId:threadId 完全相同）连续两次 ACQUIRE 均成功。
     * fencing token 须保持不变——重入不是新锁授权，不应推进 token 计数器。
     */
    @Test
    void reentrant_acquire_bySameOwner_shouldSucceedAndIncrementCount() {
        LockResult first = processor.process(acquire("lock:1", "c1", "t1", 5000));
        assertEquals(LockResult.Status.SUCCESS, first.getStatus());

        LockResult second = processor.process(acquire("lock:1", "c1", "t1", 5000));
        assertEquals(LockResult.Status.SUCCESS, second.getStatus());
        assertEquals(first.getFencingToken(), second.getFencingToken());
    }

    /**
     * 场景：加锁 3 次后释放 1 次（重入计数 3→2），锁仍由 c1 持有。
     * 部分解锁应返回 SUCCESS（操作本身成功），而非 LOCKED（避免客户端误判为"释放失败"）；
     * 此时 c2 仍不能获锁，验证部分解锁不削弱互斥语义。
     */
    @Test
    void reentrant_partialRelease_shouldDecrementCountAndKeepLocked() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult second = processor.process(acquire("lock:1", "c1", "t1", 5000));
        processor.process(acquire("lock:1", "c1", "t1", 5000));

        // Release once: reentrantTimes 3 → 2
        LockResult partial = processor.process(release("lock:1", "c1", "t1", second.getFencingToken()));

        // locked overload returns SUCCESS status with reentrantTimes
        assertEquals(LockResult.Status.SUCCESS, partial.getStatus());
        assertEquals(2L, partial.getReentrantTimes());

        // Lock still held — another owner cannot acquire
        LockResult blocked = processor.process(acquire("lock:1", "c2", "t2", 5000));
        assertEquals(LockResult.Status.LOCKED, blocked.getStatus());
    }

    /**
     * 场景：N 次加锁须 N 次解锁才真正释放，全量解锁后 c2 应能立即成功 acquire。
     * 验证"引用计数归零 → 锁真正移除"的关键边界。
     */
    @Test
    void reentrant_fullRelease_shouldRemoveLock() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult second = processor.process(acquire("lock:1", "c1", "t1", 5000));

        // Release both
        LockResult partial = processor.process(release("lock:1", "c1", "t1", second.getFencingToken()));
        assertEquals(LockResult.Status.SUCCESS, partial.getStatus());

        LockResult full = processor.process(release("lock:1", "c1", "t1", second.getFencingToken()));
        assertEquals(LockResult.Status.SUCCESS, full.getStatus());

        // Lock is now free
        LockResult newOwner = processor.process(acquire("lock:1", "c2", "t2", 5000));
        assertEquals(LockResult.Status.SUCCESS, newOwner.getStatus());
    }

    /**
     * 场景：c1 重入持锁期间 c2 尝试加锁，验证重入不会对外部 client 开放互斥边界。
     * 重入仅对同一 owner 有效，其他 owner 仍被阻塞。
     */
    @Test
    void reentrant_acquireByOtherWhileOwnerReentered_shouldReturnLocked() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));
        processor.process(acquire("lock:1", "c1", "t1", 5000));

        LockResult result = processor.process(acquire("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.LOCKED, result.getStatus());
    }

    // -------------------------------------------------------------------------
    // Performance tests
    // -------------------------------------------------------------------------

    /**
     * 场景：单线程连续 acquire+release，排除竞争噪声，测量无竞争时的吞吐上限。
     */
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
        System.out.printf("[ReentrantLock] Single-thread throughput: %.0f ops/s%n", opsPerSec);
    }

    /**
     * 场景：单线程以固定重入深度（4 层）循环测试，量化重入计数增减的额外开销。
     * 深度 D 意味着每个完整周期包含 D 次 acquire + D 次 release。
     */
    @Test
    void perf_singleThread_reentrantAcquireRelease() {
        final int iterations = 50_000;
        final int depth = 4;
        final String key = "perf:reentrant";

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            LockResult first = processor.process(acquire(key, "c1", "t1", 60_000));
            for (int d = 1; d < depth; d++) {
                processor.process(acquire(key, "c1", "t1", 60_000));
            }
            for (int d = 0; d < depth; d++) {
                processor.process(release(key, "c1", "t1", first.getFencingToken()));
            }
        }
        long elapsed = System.nanoTime() - start;

        double cyclesPerSec = iterations / (elapsed / 1_000_000_000.0);
        System.out.printf("[ReentrantLock] Reentrant depth-%d cycles/s: %.0f%n", depth, cyclesPerSec);
    }

    /**
     * 场景：8 线程竞争同一把可重入锁，互斥性决定同一时刻只有一个 acquire 成功，
     * 衡量高竞争场景下的真实有效吞吐（successful acquire+release 对数/秒）。
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
        System.out.printf("[ReentrantLock] Multi-thread (%d threads) throughput: %.0f successful acquire+release ops/s%n",
                threads, opsPerSec);
    }
}
