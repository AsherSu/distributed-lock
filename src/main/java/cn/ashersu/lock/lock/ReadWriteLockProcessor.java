package cn.ashersu.lock.lock;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.statemachine.LockEntry;
import cn.ashersu.lock.statemachine.LockResult;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 读写锁处理器，同时处理 {@link LockType#READ} 和 {@link LockType#WRITE}。
 *
 * <h3>并发规则</h3>
 * <ul>
 *   <li>读-读：允许并发，多个读锁可同时持有。</li>
 *   <li>读-写：互斥，有活跃读锁时写锁阻塞；写锁持有期间读锁阻塞。</li>
 *   <li>写-写：互斥，同一时刻只允许一个写锁。</li>
 * </ul>
 *
 * <h3>状态模型</h3>
 * 每个 lockKey 对应一个 {@link RwState}：
 * <ul>
 *   <li>{@code readers}：当前所有活跃读锁持有方（ownerId → LockEntry）。</li>
 *   <li>{@code writer}：当前写锁持有方，null 表示无写锁。</li>
 * </ul>
 */
public class ReadWriteLockProcessor implements LockProcessor {

    /**
     * 每个 lockKey 的读写锁状态
     */
    private ConcurrentHashMap<String, RwState> rwStore = new ConcurrentHashMap<>();

    /**
     * fencing token 生成器，全局单调递增
     */
    private final AtomicLong fencingTokenCounter = new AtomicLong(0);

    // -------------------------------------------------------------------------
    // LockProcessor
    // -------------------------------------------------------------------------

    @Override
    public Set<LockType> getSupportedTypes() {
        return Set.of(LockType.READ, LockType.WRITE);
    }

    @Override
    public LockResult process(LockCommand cmd) {
        switch (cmd.getType()) {
            case ACQUIRE:
                return applyAcquire(cmd);
            case RELEASE:
                return applyRelease(cmd);
            case RENEW:
                return applyRenew(cmd);
            default:
                return LockResult.error(cmd.getLockType(), "Unknown command type: " + cmd.getType());
        }
    }

    private LockResult applyAcquire(LockCommand cmd) {
        RwState state = rwStore.computeIfAbsent(cmd.getLockKey(), k -> new RwState());
        cleanExpired(state);

        if (cmd.getLockType() == LockType.READ) {
            return applyReadAcquire(cmd, state);
        } else {
            return applyWriteAcquire(cmd, state);
        }
    }

    private LockResult applyReadAcquire(LockCommand cmd, RwState state) {
        // 写锁被他人持有 → 阻塞
        if (state.writer != null) {
            long remaining = Math.max(0L, state.writer.getExpireTime() - System.currentTimeMillis());
            return LockResult.locked(LockType.READ, "Write lock held by " + state.writer.getOwnerId(), remaining);
        }

        // 幂等：自己已经持有读锁
        LockEntry existing = state.readers.get(cmd.getClientIdentify());
        if (existing != null) {
            existing.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
            existing.setReentrantTimes(existing.getReentrantTimes() + 1);
            return LockResult.success(LockType.READ, existing.getFencingToken());
        }

        // 授予读锁
        LockEntry entry = buildEntry(cmd, LockType.READ);
        state.readers.put(cmd.getClientIdentify(), entry);
        return LockResult.success(LockType.READ, entry.getFencingToken());
    }

    private LockResult applyWriteAcquire(LockCommand cmd, RwState state) {
        // 自己已持有写锁（幂等重入）
        if (state.writer != null && state.writer.getOwnerId().equals(cmd.getClientIdentify())) {
            state.writer.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
            return LockResult.success(LockType.WRITE, state.writer.getFencingToken());
        }

        // 有其他写锁 → 阻塞
        if (state.writer != null) {
            long remaining = Math.max(0L, state.writer.getExpireTime() - System.currentTimeMillis());
            return LockResult.locked(LockType.WRITE, "Write lock held by " + state.writer.getOwnerId(), remaining);
        }

        // 有活跃读锁 → 阻塞（返回最长剩余 TTL）
        if (!state.readers.isEmpty()) {
            long maxRemaining = state.readers.values().stream()
                    .mapToLong(e -> Math.max(0L, e.getExpireTime() - System.currentTimeMillis()))
                    .max().orElse(0L);
            return LockResult.locked(LockType.WRITE,
                    "Read lock(s) active, count=" + state.readers.size(), maxRemaining);
        }

        // 授予写锁
        LockEntry entry = buildEntry(cmd, LockType.WRITE);
        state.writer = entry;
        return LockResult.success(LockType.WRITE, entry.getFencingToken());
    }

    // -------------------------------------------------------------------------
    // RELEASE
    // -------------------------------------------------------------------------

    private LockResult applyRelease(LockCommand cmd) {
        RwState state = rwStore.get(cmd.getLockKey());

        if (cmd.getLockType() == LockType.READ) {
            return applyReadRelease(cmd, state);
        } else {
            return applyWriteRelease(cmd, state);
        }
    }

    private LockResult applyReadRelease(LockCommand cmd, RwState state) {
        // 锁的存在问题
        if (state == null) {
            return LockResult.success(LockType.READ, -1);
        }
        // 锁的归属
        LockEntry entry = state.readers.get(cmd.getClientIdentify());
        if (entry == null || entry.isExpired()) {
            state.readers.remove(cmd.getClientIdentify());
            return LockResult.success(LockType.READ, -1);
        }
        // 令牌
        if (entry.getFencingToken() != cmd.getFencingToken()) {
            return LockResult.stale(LockType.READ, "Fencing token mismatch");
        }
        if (entry.getReentrantTimes() <= 1) {
            // 重入层数归零，完全释放
            state.readers.remove(cmd.getClientIdentify());
            return LockResult.success(LockType.READ, entry.getFencingToken());
        } else {
            // 部分解锁：递减重入计数，读锁仍由自己持有
            entry.setReentrantTimes(entry.getReentrantTimes() - 1);
            long remaining = Math.max(0L, entry.getExpireTime() - System.currentTimeMillis());
            return LockResult.locked(LockType.READ, entry.getFencingToken(), remaining, entry.getReentrantTimes());
        }
    }

    private LockResult applyWriteRelease(LockCommand cmd, RwState state) {
        // 锁的存在问题
        if (state == null || state.writer == null || state.writer.isExpired()) {
            if (state != null) state.writer = null;
            return LockResult.success(LockType.WRITE, -1);
        }
        // 锁的归属
        if (!state.writer.getOwnerId().equals(cmd.getClientIdentify())) {
            return LockResult.stale(LockType.WRITE, "Write lock is not owned by you");
        }
        // 令牌
        if (state.writer.getFencingToken() != cmd.getFencingToken()) {
            return LockResult.stale(LockType.WRITE, "Fencing token mismatch");
        }
        LockEntry writer = state.writer;
        state.writer = null;
        return LockResult.success(LockType.WRITE, writer.getFencingToken());
    }

    // -------------------------------------------------------------------------
    // RENEW
    // -------------------------------------------------------------------------

    private LockResult applyRenew(LockCommand cmd) {
        RwState state = rwStore.get(cmd.getLockKey());

        if (cmd.getLockType() == LockType.READ) {
            return applyReadRenew(cmd, state);
        } else {
            return applyWriteRenew(cmd, state);
        }
    }

    private LockResult applyReadRenew(LockCommand cmd, RwState state) {
        if (state == null) {
            return LockResult.stale(LockType.READ, "Lock already expired");
        }
        LockEntry entry = state.readers.get(cmd.getClientIdentify());
        if (entry == null || entry.isExpired()) {
            state.readers.remove(cmd.getClientIdentify());
            return LockResult.stale(LockType.READ, "Lock already expired");
        }
        entry.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
        return LockResult.success(LockType.READ, entry.getFencingToken());
    }

    private LockResult applyWriteRenew(LockCommand cmd, RwState state) {
        if (state == null || state.writer == null || state.writer.isExpired()) {
            return LockResult.stale(LockType.WRITE, "Lock already expired");
        }
        if (!state.writer.getOwnerId().equals(cmd.getClientIdentify())) {
            return LockResult.stale(LockType.WRITE, "Write lock is not owned by you");
        }
        state.writer.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
        return LockResult.success(LockType.WRITE, state.writer.getFencingToken());
    }

    private LockEntry buildEntry(LockCommand cmd, LockType type) {
        LockEntry entry = new LockEntry();
        entry.setLockType(type);
        entry.setLockKey(cmd.getLockKey());
        entry.setOwnerId(cmd.getClientIdentify());
        entry.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
        entry.setFencingToken(fencingTokenCounter.incrementAndGet());
        // 读锁支持重入，首次加锁计数为 1
        if (type == LockType.READ) {
            entry.setReentrantTimes(1L);
        }
        return entry;
    }

    /**
     * 清理 state 中所有已过期的读锁条目和写锁
     */
    private void cleanExpired(RwState state) {
        if (state.writer != null && state.writer.isExpired()) {
            state.writer = null;
        }
        state.readers.entrySet().removeIf(e -> e.getValue().isExpired());
    }

    // -------------------------------------------------------------------------
    // Snapshot
    // -------------------------------------------------------------------------

    @Override
    public void saveSnapshot(ObjectOutputStream out) throws Exception {
        out.writeObject(rwStore);
        out.writeLong(fencingTokenCounter.get());
        out.flush();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void loadSnapshot(ObjectInputStream in) throws Exception {
        rwStore = (ConcurrentHashMap<String, RwState>) in.readObject();
        fencingTokenCounter.set(in.readLong());
    }

    /**
     * 单个 lockKey 的读写锁运行时状态，保存于 {@link #rwStore}。
     */
    static class RwState implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * 当前所有活跃读锁：ownerId(clientId:threadId) → LockEntry
         */
        final Map<String, LockEntry> readers = new HashMap<>();

        /**
         * 当前写锁持有方；null 表示无写锁
         */
        LockEntry writer;
    }
}
