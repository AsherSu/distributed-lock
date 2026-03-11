package cn.ashersu.lock.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 锁续期守护线程，防止持锁方因业务耗时过长导致锁自然过期。
 *
 * <h3>工作原理</h3>
 * <ul>
 *   <li>以 TTL/3 的频率向 Raft 集群发送 RENEW 命令。</li>
 *   <li>连续续期失败 {@link #MAX_RENEW_FAIL_COUNT} 次后，认为锁已丢失，
 *       回调 {@link LockLostListener} 通知业务层处理（例如回滚事务、停止写入）。</li>
 * </ul>
 *
 * <h3>使用方式</h3>
 * <pre>
 *   LockWatchdog watchdog = new LockWatchdog(client, "order:pay:1001", clientId, 30_000L);
 *   watchdog.setLockLostListener(() -> log.error("Lock lost! Rollback now."));
 *   Thread t = new Thread(watchdog, "lock-watchdog");
 *   t.setDaemon(true);  // 设为守护线程，主程序退出时自动结束
 *   t.start();
 *   // ... 业务逻辑 ...
 *   watchdog.stop();
 * </pre>
 */
public class LockWatchdog implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LockWatchdog.class);

    /** 连续续期失败多少次后放弃锁。 */
    private static final int MAX_RENEW_FAIL_COUNT = 3;

    private final DistributedLockClient client;
    private final String lockKey;
    private final String clientId;

    /** 锁的 TTL（毫秒），续期间隔 = ttlMs / 3。 */
    private final long ttlMs;

    /** 续期间隔（毫秒）。 */
    private final long intervalMs;

    /** 控制线程循环的标志，volatile 保证可见性。 */
    private volatile boolean running = true;

    /** 连续失败计数器。 */
    private int failCount = 0;

    /** 锁丢失时的回调（可选）。 */
    private LockLostListener lockLostListener;

    public LockWatchdog(DistributedLockClient client, String lockKey, String clientId, long ttlMs) {
        this.client = client;
        this.lockKey = lockKey;
        this.clientId = clientId;
        this.ttlMs = ttlMs;
        this.intervalMs = ttlMs / 3;
    }

    /**
     * 守护线程主循环。
     *
     * <h3>实现逻辑</h3>
     * <ol>
     *   <li>循环条件：{@code running && !Thread.currentThread().isInterrupted()}。</li>
     *   <li>每轮开始先 sleep(intervalMs)。</li>
     *   <li>调用 {@code client.renew(lockKey, clientId, ttlMs)}：
     *     <ul>
     *       <li>成功 → failCount = 0，继续。</li>
     *       <li>失败 → failCount++，如果达到 MAX_RENEW_FAIL_COUNT 则：
     *           running = false，触发 lockLostListener，退出循环。</li>
     *     </ul>
     *   </li>
     * </ol>
     */
    @Override
    public void run() {
        // TODO: 实现续期循环（见上方 Javadoc）
        throw new UnsupportedOperationException("TODO: 实现 LockWatchdog.run()");
    }

    /** 停止续期线程（在 unlock 成功后调用）。 */
    public void stop() {
        this.running = false;
    }

    public void setLockLostListener(LockLostListener listener) {
        this.lockLostListener = listener;
    }

    /** 锁丢失回调接口。 */
    @FunctionalInterface
    public interface LockLostListener {
        void onLockLost();
    }
}
