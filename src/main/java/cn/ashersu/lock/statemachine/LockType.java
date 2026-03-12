package cn.ashersu.lock.statemachine;

/**
 * 分布式锁的类型枚举。
 */
public enum LockType {

    /**
     * 基础互斥锁。
     * 同一时刻只允许一个客户端持有，不支持重入。
     * 已在状态机中实现（reentrantTimes 固定为 1）。
     */
    MUTEX("互斥锁", false, false, 1),

    /**
     * 可重入锁。
     * 持锁方可多次申请同一把锁，引用计数 +1；释放时计数 -1，归零才真正解锁。
     * 已在状态机中实现（reentrantTimes > 1）。
     */
    REENTRANT("可重入锁", true, false, 1),

    /**
     * 公平锁（待实现，优先级：高）。
     * 以 FIFO 队列管理等待者，保证先到先得，避免请求饥饿。
     * 实现要点：在 LockEntry 中维护等待队列，ACQUIRE 时将 clientId 入队；
     * RELEASE 后取队头授予，而非任意竞争。
     */
    FAIR("公平锁", true, true, 1),

    /**
     * 读锁（待实现，优先级：中）。
     * 读写锁对中的共享端。多个读锁可同时持有，写锁出现时所有读锁阻塞。
     * 实现要点：LockEntry 增加 readerCount；ACQUIRE_READ 时检查是否有活跃写锁。
     */
    READ("读锁", true, false, Integer.MAX_VALUE),

    /**
     * 写锁（待实现，优先级：中）。
     * 读写锁对中的独占端。与任意读锁或写锁互斥。
     * 实现要点：与 READ 类型共享同一个 lockKey 的 LockEntry，检查 readerCount == 0。
     */
    WRITE("写锁", true, false, 1),

    /**
     * 信号量（待实现，优先级：中）。
     * 允许最多 {@code maxPermits} 个客户端同时持有，超出则等待或失败。
     * 实现要点：LockEntry 增加 permits / maxPermits 字段；ACQUIRE 时 permits-- ，
     * RELEASE 时 permits++。
     */
    SEMAPHORE("信号量", false, false, Integer.MAX_VALUE),

    /**
     * 联合锁 MultiLock（待实现，优先级：低）。
     * 一次性原子地申请多个不同 key 的锁，全部成功才算获取，任意失败则全部回滚。
     * 实现要点：客户端聚合多条 ACQUIRE 命令为一个原子批次写入 Raft 日志，
     * 状态机按批次全量检查后再统一写入。
     */
    MULTI("MultiLock", false, false, Integer.MAX_VALUE);

    /** 锁类型的中文描述。 */
    private final String description;

    /**
     * 是否支持可重入（同一 ownerId 可重复 ACQUIRE，引用计数递增）。
     */
    private final boolean reentrant;

    /**
     * 是否需要公平排队（FIFO 等待队列）。
     */
    private final boolean fair;

    /**
     * 同一 lockKey 允许的最大持锁方数量。
     * 互斥/公平/读写写端 = 1，读端/信号量 = N（Integer.MAX_VALUE 表示由运行时参数决定）。
     */
    private final int maxConcurrency;

    LockType(String description, boolean reentrant, boolean fair, int maxConcurrency) {
        this.description = description;
        this.reentrant = reentrant;
        this.fair = fair;
        this.maxConcurrency = maxConcurrency;
    }

    public String getDescription() {
        return description;
    }

    public boolean isReentrant() {
        return reentrant;
    }

    public boolean isFair() {
        return fair;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    /** 是否为读写锁体系的成员（READ 或 WRITE）。 */
    public boolean isReadWriteLock() {
        return this == READ || this == WRITE;
    }

    /** 是否允许多方同时持有（信号量或读锁）。 */
    public boolean isShared() {
        return maxConcurrency > 1;
    }
}
