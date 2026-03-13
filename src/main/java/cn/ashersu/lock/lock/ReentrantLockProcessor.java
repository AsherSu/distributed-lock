package cn.ashersu.lock.lock;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.statemachine.LockEntry;
import cn.ashersu.lock.statemachine.LockResult;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ReentrantLockProcessor implements LockProcessor{

    // 互斥锁存储map
    private ConcurrentHashMap<String, LockEntry> reentrantLockStore = new ConcurrentHashMap<>();

    // 围栏令牌生成器，全局单调递增。
    private final AtomicLong fencingTokenCounter = new AtomicLong(0);

    @Override
    public Set<LockType> getSupportedTypes() {
        return Collections.singleton(LockType.REENTRANT);
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
        LockEntry existing = reentrantLockStore.get(cmd.getLockKey());
        if (existing == null || existing.isExpired()) {
            // 无锁或锁已过期，授予新锁
            reentrantLockStore.remove(cmd.getLockKey());
            LockEntry lockNew = new LockEntry();
            lockNew.setLockType(cmd.getLockType());
            lockNew.setLockKey(cmd.getLockKey());
            lockNew.setOwnerId(cmd.getClientIdentify());
            lockNew.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
            lockNew.setFencingToken(fencingTokenCounter.incrementAndGet());
            lockNew.setReentrantTimes(1L);
            reentrantLockStore.put(cmd.getLockKey(), lockNew);
            return LockResult.success(cmd.getLockType(), lockNew.getFencingToken());
        } else if (existing.getOwnerId().equals(cmd.getClientIdentify())) {
            // 同一持锁方重入：递增计数，重置过期时间
            existing.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
            existing.setReentrantTimes(existing.getReentrantTimes() + 1);
            return LockResult.success(cmd.getLockType(), existing.getFencingToken());
        } else {
            // 他人持有，返回剩余 TTL 供客户端决策等待时长
            long remaining = Math.max(0L, existing.getExpireTime() - System.currentTimeMillis());
            return LockResult.locked(cmd.getLockType(), "Lock held by " + existing.getOwnerId(), remaining);
        }
    }

    /**
     * 释放锁
     */
    private LockResult applyRelease(LockCommand cmd) {
        LockEntry existing = reentrantLockStore.get(cmd.getLockKey());
        if (existing == null || existing.isExpired()) {
            // 锁不存在或已过期，清理并视为释放成功
            reentrantLockStore.remove(cmd.getLockKey());
            return LockResult.success(cmd.getLockType(), -1);
        } else if (!existing.getOwnerId().equals(cmd.getClientIdentify())) {
            // 非当前用户的锁
            return LockResult.stale(cmd.getLockType(), "lock is not beyond you");
        } else if (existing.getFencingToken() != cmd.getFencingToken()) {
            // 令牌不一致
            return LockResult.stale(cmd.getLockType(), "fencing token do not same");
        } else {
            if (existing.getReentrantTimes() <= 1) {
                // 完全解锁
                reentrantLockStore.remove(cmd.getLockKey());
                return LockResult.success(cmd.getLockType(), existing.getFencingToken());
            } else {
                // 部分解锁：重入次数递减，锁仍由自己持有
                existing.setReentrantTimes(existing.getReentrantTimes() - 1);
                long remaining = Math.max(0L, existing.getExpireTime() - System.currentTimeMillis());
                return LockResult.locked(cmd.getLockType(), existing.getFencingToken(), remaining, existing.getReentrantTimes());
            }
        }
    }

    /**
     * 锁续期
     */
    private LockResult applyRenew(LockCommand cmd) {
        LockEntry existing = reentrantLockStore.get(cmd.getLockKey());
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
        out.writeObject(reentrantLockStore);
        out.flush();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void loadSnapshot(ObjectInputStream in) throws Exception {
        reentrantLockStore = (ConcurrentHashMap<String, LockEntry>) in.readObject();
    }
}
