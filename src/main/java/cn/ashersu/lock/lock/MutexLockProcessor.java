package cn.ashersu.lock.lock;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.statemachine.LockEntry;
import cn.ashersu.lock.statemachine.LockResult;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MutexLockProcessor implements LockProcessor{

    // 互斥锁存储map
    private ConcurrentHashMap<String, LockEntry> mutexLockStore = new ConcurrentHashMap<>();

    // 围栏令牌生成器，全局单调递增。
    private final AtomicLong fencingTokenCounter = new AtomicLong(0);

    @Override
    public LockType getSupportedType() {
        return LockType.MUTEX;
    }

    @Override
    public LockResult process(LockCommand cmd){
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

    /**
     * 处理 ACQUIRE 命令（申请锁）。
     */
    private LockResult applyAcquire(LockCommand cmd) {
        LockEntry existing = mutexLockStore.get(cmd.getLockKey());
        if (existing == null || existing.isExpired()) {
            // 无锁
            LockEntry lockNew = new LockEntry();
            lockNew.setLockKey(cmd.getLockKey());
            lockNew.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
            lockNew.setOwnerId(cmd.getClientId());
            lockNew.setReentrantTimes(1L);
            lockNew.setFencingToken(fencingTokenCounter.incrementAndGet());
            mutexLockStore.put(cmd.getLockKey(), lockNew);
            return LockResult.success(cmd.getLockType(), lockNew.getFencingToken());
        } else if (existing.getOwnerId().equals(cmd.getClientId())) {
            // 锁重入
            existing.setReentrantTimes(existing.getReentrantTimes() + 1);
            existing.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
            return LockResult.success(cmd.getLockType(), existing.getFencingToken());
        } else {
            // 他人持有，返回剩余 TTL 供客户端等待
            long remaining = Math.max(0L, existing.getExpireTime() - System.currentTimeMillis());
            return LockResult.locked(cmd.getLockType(), "Lock held by " + existing.getOwnerId(), remaining, existing.getReentrantTimes());
        }
    }

    /**
     * 释放锁
     */
    private LockResult applyRelease(LockCommand cmd) {
        LockEntry existing = mutexLockStore.get(cmd.getLockKey());
        if (existing == null || existing.isExpired()) {
            // 锁不存在或已过期，清理并视为释放成功
            mutexLockStore.remove(cmd.getLockKey());
            return LockResult.success(cmd.getLockType(), -1);
        } else if (!existing.getOwnerId().equals(cmd.getClientIdentify())) {
            // 非当前用户的锁
            return LockResult.stale(cmd.getLockType(), "lock is not beyond you");
        } else if (existing.getFencingToken() != cmd.getFencingToken()) {
            // 令牌不一致
            return LockResult.stale(cmd.getLockType(), "fencing token do not same");
        } else {
            if (existing.getReentrantTimes() == 1) {
                // 解锁
                mutexLockStore.remove(cmd.getLockKey());
                return LockResult.success(cmd.getLockType(), existing.getFencingToken());
            } else {
                // 重入锁解锁
                existing.setReentrantTimes(existing.getReentrantTimes() - 1);
                long expireTime = existing.getExpireTime() - System.currentTimeMillis();
                return LockResult.locked(cmd.getLockType(), "unLock reentrant lock", expireTime, existing.getReentrantTimes());
            }
        }
    }

    /**
     * 锁续期
     */
    private LockResult applyRenew(LockCommand cmd) {
        LockEntry existing = mutexLockStore.get(cmd.getLockKey());
        if (existing == null || existing.isExpired()) {
            // 锁不存在
            return LockResult.stale(cmd.getLockType(), "Lock already expired");
        } else if (!existing.getOwnerId().equals(cmd.getClientIdentify())) {
            // 非当前用户的锁
            return LockResult.stale(cmd.getLockType(), "Lock do not beyond you");
        } else {
            existing.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
            return LockResult.success(cmd.getLockType(), existing.getFencingToken());
        }
    }

    @Override
    public void saveSnapshot(ObjectOutputStream out) throws Exception {
        out.writeObject(mutexLockStore);
        out.flush();
    }

    @Override
    public void loadSnapshot(ObjectInputStream in) throws Exception {
        mutexLockStore = (ConcurrentHashMap<String, LockEntry>) in.readObject();
    }
}
