package cn.ashersu.lock.lock;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.statemachine.LockEntry;
import cn.ashersu.lock.statemachine.LockResult;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class FairLockProcessor implements LockProcessor {

    // 公平锁存储map
    private ConcurrentHashMap<String, LockEntry> fairLockStore = new ConcurrentHashMap<>();

    // 公平锁排队队列map
    private ConcurrentHashMap<String, LinkedList<LockEntry>> fairMap = new ConcurrentHashMap<>();

    // 围栏令牌生成器，全局单调递增。
    private final AtomicLong fencingTokenCounter = new AtomicLong(0);

    {
        // 定时任务删除过期排队
        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (ConcurrentHashMap.Entry<String, LinkedList<LockEntry>> lockEntry : fairMap.entrySet()) {
                for (LockEntry lock : lockEntry.getValue()) {
                    //过期删除
                    if (lock.getWaitTime() <= System.currentTimeMillis()) {
                        fairMap.get(lock.getLockKey()).remove(lock);
                    }
                }
            }
        });
        thread.start();
    }

    @Override
    public LockType getSupportedType() {
        return LockType.FAIR;
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

    private LockResult applyRelease(LockCommand cmd) {
        LockEntry existing = fairLockStore.get(cmd.getLockKey());

        // 锁不存在或已过期：清理并尝试推进队列
        if (existing == null || existing.isExpired()) {
            fairLockStore.remove(cmd.getLockKey());
            removeFromQueue(cmd.getLockKey(), cmd.getClientIdentify());
            promoteNextWaiter(cmd.getLockKey());
            return LockResult.success(cmd.getLockType(), -1);
        }

        // 锁不属于请求方：若在队列中则退出排队，否则返回 STALE
        if (!existing.getOwnerId().equals(cmd.getClientIdentify())) {
            boolean removed = removeFromQueue(cmd.getLockKey(), cmd.getClientIdentify());
            if (removed) {
                return LockResult.success(cmd.getLockType(), -1);
            }
            return LockResult.stale(cmd.getLockType(), "Lock is not owned by you");
        }

        // fencing token 校验，防止过期重放
        if (existing.getFencingToken() != cmd.getFencingToken()) {
            return LockResult.stale(cmd.getLockType(), "Fencing token mismatch");
        }

        // 属于自己的锁：释放并将队首等待者晋升为新持锁方
        fairLockStore.remove(cmd.getLockKey());
        promoteNextWaiter(cmd.getLockKey());
        return LockResult.success(cmd.getLockType(), existing.getFencingToken());
    }

    /**
     * 将锁的队首非过期等待者提升为新的持锁方，写入 fairLockStore。
     * 已超过 waitTime 的队头条目会被跳过并丢弃。
     */
    private void promoteNextWaiter(String lockKey) {
        LinkedList<LockEntry> queue = fairMap.get(lockKey);
        if (queue == null) {
            return;
        }
        // 跳过已超时的等待者
        LockEntry next;
        while ((next = queue.peek()) != null) {
            if (next.getWaitTime() > 0 && next.getWaitTime() <= System.currentTimeMillis()) {
                queue.poll();
            } else {
                break;
            }
        }
        if (next == null) {
            return;
        }
        queue.poll();
        next.setFencingToken(fencingTokenCounter.incrementAndGet());
        // 重置过期时间：以当前时刻起算，保留原始 TTL 时长
        next.setExpireTime(System.currentTimeMillis() + next.getExpireTime());
        fairLockStore.put(lockKey, next);
    }

    /**
     * 从等待队列中移除指定 ownerId 的条目。
     *
     * @return 是否实际移除了至少一条
     */
    private boolean removeFromQueue(String lockKey, String ownerId) {
        LinkedList<LockEntry> queue = fairMap.get(lockKey);
        if (queue == null) {
            return false;
        }
        return queue.removeIf(e -> e.getLockKey().equals(lockKey) && e.getOwnerId().equals(ownerId));
    }

    private LockResult applyAcquire(LockCommand cmd) {
        LockEntry existing = fairLockStore.get(cmd.getLockKey());

        // 持锁记录已过期：清理并推进队列（过期的锁不能阻塞等待者）
        if (existing != null && existing.isExpired()) {
            fairLockStore.remove(cmd.getLockKey());
            promoteNextWaiter(cmd.getLockKey());
            existing = fairLockStore.get(cmd.getLockKey());
        }

        // 锁空闲
        if (existing == null) {
            LinkedList<LockEntry> queue = fairMap.get(cmd.getLockKey());
            if (queue == null || queue.isEmpty()) {
                // 无人排队，直接授锁
                return grantLock(cmd);
            }
            // 公平性：队列非空则新请求者必须排队，不能插队
            return enqueueWaiter(cmd);
        }

        // 幂等：自己已持有锁（网络重试），直接返回 success
        if (existing.getOwnerId().equals(cmd.getClientIdentify())) {
            return LockResult.success(cmd.getLockType(), existing.getFencingToken());
        }

        // 锁被他人持有：幂等检查——若已在队列中则不重复入队
        LinkedList<LockEntry> queue = fairMap.computeIfAbsent(cmd.getLockKey(), k -> new LinkedList<>());
        boolean alreadyQueued = queue.stream().anyMatch(e -> e.getOwnerId().equals(cmd.getClientIdentify()));
        if (alreadyQueued) {
            long remaining = Math.max(0L, existing.getExpireTime() - System.currentTimeMillis());
            return LockResult.locked(cmd.getLockType(), "Already waiting in queue", remaining);
        }

        return enqueueWaiter(cmd);
    }

    private LockResult grantLock(LockCommand cmd) {
        LockEntry lockNew = buildEntry(cmd);
        lockNew.setFencingToken(fencingTokenCounter.incrementAndGet());
        fairLockStore.put(cmd.getLockKey(), lockNew);
        return LockResult.success(cmd.getLockType(), lockNew.getFencingToken());
    }

    private LockResult enqueueWaiter(LockCommand cmd) {
        LockEntry lockNew = buildEntry(cmd);
        lockNew.setWaitTime(System.currentTimeMillis() + cmd.getWaitTime());
        fairMap.computeIfAbsent(cmd.getLockKey(), k -> new LinkedList<>()).add(lockNew);
        LockEntry holder = fairLockStore.get(cmd.getLockKey());
        long remaining = holder != null ? Math.max(0L, holder.getExpireTime() - System.currentTimeMillis()) : 0L;
        return LockResult.locked(cmd.getLockType(), "Lock held by " + (holder != null ? holder.getOwnerId() : "queue"), remaining);
    }

    private LockEntry buildEntry(LockCommand cmd) {
        LockEntry entry = new LockEntry();
        entry.setLockType(cmd.getLockType());
        entry.setLockKey(cmd.getLockKey());
        entry.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
        entry.setOwnerId(cmd.getClientIdentify());
        return entry;
    }

    private LockResult applyRenew(LockCommand cmd) {
        LockEntry existing = fairLockStore.get(cmd.getLockKey());
        if (existing == null || existing.isExpired()) {
            return LockResult.stale(cmd.getLockType(), "Lock already expired");
        }
        if (!existing.getOwnerId().equals(cmd.getClientIdentify())) {
            return LockResult.stale(cmd.getLockType(), "Lock is not owned by you");
        }
        existing.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
        return LockResult.success(cmd.getLockType(), existing.getFencingToken());
    }

    @Override
    public void saveSnapshot(ObjectOutputStream out) throws Exception {

    }

    @Override
    public void loadSnapshot(ObjectInputStream in) throws Exception {

    }
}
