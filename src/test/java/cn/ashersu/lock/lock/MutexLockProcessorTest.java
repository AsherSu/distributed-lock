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

    /**
     * 场景：无竞争情况下首次加锁。
     * fencing token 必须为正数，它是后续单调递增比较的基准值，为零则下游无法用它排序。
     */
    @Test
    void acquire_whenFree_shouldSucceedWithPositiveFencingToken() {
        LockResult result = processor.process(acquire("lock:1", "c1", "t1", 5000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertTrue(result.getFencingToken() > 0);
    }

    /**
     * 场景：锁被 c1 持有期间 c2 尝试加锁。
     * 互斥语义核心验证：LOCKED 响应须携带剩余 TTL，客户端可据此决定是立即失败还是等待重试。
     */
    @Test
    void acquire_whenHeldByOther_shouldReturnLocked() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));

        LockResult result = processor.process(acquire("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.LOCKED, result.getStatus());
        assertTrue(result.getRemainingTtlMs() > 0);
    }

    /**
     * 场景：c1 的锁 TTL 到期后 c2 来 ACQUIRE，触发懒清理（过期锁不主动删除，在下次操作时替换）。
     * 验证过期路径能正常授锁，避免"永久死锁"。
     */
    @Test
    void acquire_whenExpired_shouldGrantNewLock() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50));
        Thread.sleep(100);

        LockResult result = processor.process(acquire("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertTrue(result.getFencingToken() > 0);
    }

    /**
     * 场景：同一 key 经历一次完整的 acquire-release 后再次加锁。
     * fencing token 必须严格递增，下游系统用它拒绝来自"旧锁实例"的过期操作请求。
     */
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

    /**
     * 场景：标准释放路径，owner + fencing token 双重校验均通过。
     */
    @Test
    void release_byOwnerWithCorrectToken_shouldSucceed() {
        LockResult acquired = processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(release("lock:1", "c1", "t1", acquired.getFencingToken()));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    /**
     * 场景：c2 持有正确的 fencing token 却不是 owner，尝试释放 c1 的锁。
     * 防止客户端崩溃重启后误用旧 key 释放其他 client 正在持有的锁。
     */
    @Test
    void release_byNonOwner_shouldReturnStale() {
        LockResult acquired = processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(release("lock:1", "c2", "t2", acquired.getFencingToken()));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    /**
     * 场景：owner 携带错误的 fencing token 尝试释放锁。
     * 防止 STW/Full-GC 后过期客户端实例持旧 token 错误释放当前有效锁（新锁已被他人获取）。
     */
    @Test
    void release_withWrongFencingToken_shouldReturnStale() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(release("lock:1", "c1", "t1", 9999L));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    /**
     * 场景：锁已自然过期后 owner 仍发来 RELEASE 请求（watchdog 续期失败、网络延迟等场景）。
     * 过期锁的释放应幂等成功，客户端无需区分"释放成功"与"锁已过期"。
     */
    @Test
    void release_whenExpired_shouldSucceed() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50));
        Thread.sleep(100);

        LockResult result = processor.process(release("lock:1", "c1", "t1", 1L));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    /**
     * 场景：对从未加锁的 key 执行释放（如客户端重启后补偿调用 unlock）。
     * 幂等语义保证客户端重试不因"锁不存在"而报错中断流程。
     */
    @Test
    void release_whenNeverAcquired_shouldSucceed() {
        LockResult result = processor.process(release("lock:absent", "c1", "t1", 1L));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
    }

    // -------------------------------------------------------------------------
    // Renew tests
    // -------------------------------------------------------------------------

    /**
     * 场景：watchdog 正常续期，仅延长 TTL，fencing token 保持不变。
     * token 不变意味着下游视角下持锁方未发生变更，续期不触发任何权限重置。
     */
    @Test
    void renew_byOwner_shouldSucceedAndExtendExpiry() {
        LockResult acquired = processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(renew("lock:1", "c1", "t1", 10000));

        assertEquals(LockResult.Status.SUCCESS, result.getStatus());
        assertEquals(acquired.getFencingToken(), result.getFencingToken());
    }

    /**
     * 场景：非 owner 尝试续期他人的锁（误操作或攻击）。
     * 续期权限与持锁权等价，非 owner 不得延长锁的存活时间。
     */
    @Test
    void renew_byNonOwner_shouldReturnStale() {
        processor.process(acquire("lock:1", "c1", "t1", 5000));
        LockResult result = processor.process(renew("lock:1", "c2", "t2", 5000));

        assertEquals(LockResult.Status.STALE, result.getStatus());
    }

    /**
     * 场景：watchdog 续期与锁过期的竞态——续期请求到达时锁已过期（可能已被新 owner 获取）。
     * 拒绝续期可防止"僵尸续期"覆盖新锁的有效期。
     */
    @Test
    void renew_whenExpired_shouldReturnStale() throws InterruptedException {
        processor.process(acquire("lock:1", "c1", "t1", 50));
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
     * 场景：单线程连续 acquire+release，排除多线程竞争的噪声。
     * 测量无竞争时状态机的纯路径吞吐上限，作为其他性能用例的对比基准。
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
        System.out.printf("[MutexLock] Single-thread throughput: %.0f ops/s%n", opsPerSec);
    }

    /**
     * 场景：8 线程同时竞争同一把互斥锁。
     * 互斥性决定同一时刻只有一个 acquire 能成功，其余 7 个拿到 LOCKED；
     * 此指标反映高竞争下的实际有效吞吐（successful acquire+release 对数/秒）。
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
        System.out.printf("[MutexLock] Multi-thread (%d threads) throughput: %.0f successful acquire+release ops/s%n",
                threads, opsPerSec);
    }
}
