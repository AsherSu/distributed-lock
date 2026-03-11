package cn.ashersu.lock.statemachine;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.command.LockCommandType;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.SynchronizedClosure;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 分布式锁状态机。
 *
 * <h2>核心职责</h2>
 * <ol>
 *   <li>持有内存中的锁状态表 {@link #lockStore}。</li>
 *   <li>按 Raft 日志顺序逐条应用 {@link LockCommand}（写路径）。</li>
 *   <li>支持 Snapshot 保存与加载，使节点可从崩溃中恢复。</li>
 * </ol>
 *
 * <h2>关键约束</h2>
 * <ul>
 *   <li>{@link #onApply} 必须是确定性的——相同的命令序列在任何节点上执行结果必须完全一致。
 *       不能在此方法中读取系统时钟以外的任何外部状态，也不能有随机行为。</li>
 *   <li>不能在 {@link #onApply} 内部做阻塞 IO，否则会卡住整个 Raft 应用线程。</li>
 * </ul>
 */
public class LockStateMachine extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(LockStateMachine.class);

    /**
     * 内存锁表：key = lockKey，value = 当前持锁状态。
     * 线程安全由 Raft 单线程应用日志保证（onApply 串行执行），
     * 但读路径（如 ReadIndex 查询）可能并发访问，因此使用 ConcurrentHashMap。
     */
    private final ConcurrentHashMap<String, LockEntry> lockStore = new ConcurrentHashMap<>();

    /**
     * 围栏令牌生成器，全局单调递增。
     * 每次成功授予锁时执行 incrementAndGet()，并将新值写入 LockEntry。
     */
    private final AtomicLong fencingTokenCounter = new AtomicLong(0);

    // -------------------------------------------------------------------------
    // 写路径：Raft 日志应用（最核心的方法，请从这里开始实现）
    // -------------------------------------------------------------------------

    /**
     * Raft 框架在日志被多数节点确认后，调用此方法将日志条目应用到状态机。
     */
    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
          LockCommand cmd = null;
          LockClosure done = (LockClosure) iter.done();  // Follower 上为 null
          try {
              ByteBuffer data = iter.getData();
              cmd = LockCommand.decode(data.array());
          } catch (Exception e) {
              iter.setErrorAndRollback(1, new Status(RaftError.ESTATEMACHINE, e.getMessage()));
              return;
          }
          LockResult result = applyCommand(cmd);
          if (done != null) {
              done.setResult(result);
              done.run(Status.OK());
          }
          iter.next();
      }
    }

    /**
     * 根据命令类型分派到具体处理逻辑。
     */
    private LockResult applyCommand(LockCommand cmd) {
        switch (cmd.getType()) {
            case ACQUIRE:
                return applyAcquire(cmd);
            case RELEASE:
                return applyRelease(cmd);
            case RENEW:
                return applyRenew(cmd);
            default:
                return LockResult.error("Unknown command type: " + cmd.getType());
        }
    }

    /**
     * 处理 ACQUIRE 命令（申请锁）。
     *
     * <h3>实现逻辑</h3>
     * <ol>
     *   <li>查询 lockStore.get(cmd.getLockKey())。</li>
     *   <li>如果 entry == null 或 entry.isExpired() → 授予锁：
     *     <ul>
     *       <li>生成新令牌：long token = fencingTokenCounter.incrementAndGet()</li>
     *       <li>构造 LockEntry：expireTime = System.currentTimeMillis() + cmd.getTtlMs()</li>
     *       <li>写入 lockStore</li>
     *       <li>return LockResult.success(token)</li>
     *     </ul>
     *   </li>
     *   <li>否则 → 拒绝：return LockResult.locked("Lock held by " + entry.getOwnerId())</li>
     * </ol>
     *
     * <p><strong>幂等性处理</strong>（进阶）：可在状态机中维护一个
     * {@code Map<String, LockResult> processedRequests}，
     * 先查 requestId 是否已处理过，若是直接返回缓存结果，避免网络重传导致重复授予。
     */
    private LockResult applyAcquire(LockCommand cmd) {
        LockEntry existing = lockStore.get(cmd.getLockKey());
        if (existing == null || existing.isExpired()) {
            // 无锁
            LockEntry lockNew = new LockEntry();
            lockNew.setLockKey(cmd.getLockKey());
            lockNew.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
            lockNew.setOwnerId(cmd.getClientId());
            lockNew.setReentrantTimes(1L);
            lockNew.setFencingToken(fencingTokenCounter.incrementAndGet());
            lockStore.put(cmd.getLockKey(), lockNew);
            return LockResult.success(lockNew.getFencingToken());
        } else if (existing.getOwnerId().equals(cmd.getClientId())) {
            //锁重入
            existing.setReentrantTimes(existing.getReentrantTimes() + 1);
            existing.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
            return LockResult.success(existing.getFencingToken());
        } else {
            //他人持有
            return LockResult.locked("Lock held by " + existing.getOwnerId());
        }
    }

    /**
     * 释放锁
     */
    private LockResult applyRelease(LockCommand cmd) {
        LockEntry existing = lockStore.get(cmd.getLockKey());
        if (existing == null || existing.isExpired()) {
            // 锁不存在
            return LockResult.success(existing.getFencingToken());
        } else if (!existing.getOwnerId().equals(cmd.getClientIdentify())) {
            // 非当前用户的锁
            return LockResult.stale("");
        } else if (existing.getFencingToken() == cmd.getFencingToken()){
            // 令牌不一致
            return LockResult.stale("fencing token do not same");
        } else  {
            if (existing.getReentrantTimes() == 1){
                // 解锁
                lockStore.remove(cmd.getLockKey());
                return LockResult.success(existing.getFencingToken());
            }else {
                // 重入锁解锁
                existing.setReentrantTimes(existing.getReentrantTimes() - 1);
                return LockResult.locked("unLock reentrant lock");
            }
        }
    }

    /**
     * 锁续期
     */
    private LockResult applyRenew(LockCommand cmd) {
        LockEntry existing = lockStore.get(cmd.getLockKey());
        if (existing == null || existing.isExpired()){
            // 锁不存在
            return LockResult.stale("Lock already expired");
        } else if (!existing.getOwnerId().equals(cmd.getClientIdentify())) {
            // 非当前用户的锁
            return LockResult.stale("Lock do not beyond you");
        }else {
            existing.setExpireTime(System.currentTimeMillis() + cmd.getTtlMs());
            return LockResult.success(existing.getFencingToken());
        }
    }

    // -------------------------------------------------------------------------
    // 快照：持久化与恢复（Raft 用于日志压缩和节点快速追赶）
    // -------------------------------------------------------------------------

    /**
     * 保存快照：将当前 lockStore + fencingTokenCounter 序列化到磁盘。
     *
     * <h3>实现步骤</h3>
     * <ol>
     *   <li>获取快照文件路径：{@code writer.getPath() + "/lock_snapshot.data"}。</li>
     *   <li>将 lockStore（Map）和 fencingTokenCounter 的当前值序列化（Hessian / ObjectOutputStream）写入文件。</li>
     *   <li>调用 {@code writer.addFile("lock_snapshot.data")} 告知 jraft 此文件需要被管理。</li>
     *   <li>成功后调用 {@code done.run(Status.OK())}，失败调用 {@code done.run(new Status(RaftError.EIO, ...))}。</li>
     * </ol>
     *
     * <p><strong>注意</strong>：此方法在独立的快照线程中执行，
     * 此时 onApply 可能同时在运行（Raft 框架保证 onApply 已推进到 snapshotIndex 才触发此回调），
     * 如果你的 lockStore 是普通 HashMap，需要在此处加锁或先做深拷贝。
     */
    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        // 1. Get the snapshot file path
        String snapshotPath = writer.getPath() + "/lock_snapshot.data";
        
        try (FileOutputStream fos = new FileOutputStream(snapshotPath);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            
            // 2. Serialize lockStore and fencingTokenCounter
            // We create a wrapper object or write them sequentially. 
            // Writing sequentially: first the counter, then the map.
            oos.writeLong(fencingTokenCounter.get());
            oos.writeObject(lockStore);
            
            // 3. Add the file to the snapshot writer
            if (writer.addFile("lock_snapshot.data")) {
                // 4. Success callback
                done.run(Status.OK());
            } else {
                done.run(new Status(RaftError.EIO, "Failed to add snapshot file"));
            }
        } catch (Exception e) {
            LOG.error("Failed to save snapshot", e);
            // 4. Failure callback
            done.run(new Status(RaftError.EIO, "Failed to save snapshot: " + e.getMessage()));
        }
    }

    /**
     * 加载快照：节点启动或落后太多时，从磁盘快照恢复状态。
     *
     * <h3>实现步骤</h3>
     * <ol>
     *   <li>获取快照文件路径：{@code reader.getPath() + "/lock_snapshot.data"}。</li>
     *   <li>反序列化恢复 lockStore 和 fencingTokenCounter 的值。</li>
     *   <li>成功返回 true，失败返回 false（jraft 会标记节点为 ERROR 状态）。</li>
     * </ol>
     */
    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        // 1. Get the snapshot file path
        String snapshotPath = reader.getPath() + "/lock_snapshot.data";
        try (FileInputStream fis = new FileInputStream(snapshotPath);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            fencingTokenCounter.set(ois.readLong());
            lockStore.clear();
            lockStore.putAll((Map<String, LockEntry>) ois.readObject());
            return true;
        } catch (Exception e) {
            LOG.error("Failed to load snapshot", e);
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // 读路径：线性一致读（查询锁状态，可选实现）
    // -------------------------------------------------------------------------

    /**
     * 查询指定 lockKey 当前的持锁状态（供 ReadIndex 读使用）。
     *
     * <p>此方法本身不走 Raft 日志，但调用前需要通过
     * {@code node.readIndex(requestContext, readIndexClosure)} 确认当前节点仍是 Leader
     * 且已应用到最新的 commitIndex，才能保证读到的是线性一致的结果。
     *
     * @return null 表示锁不存在或已过期
     */
    public LockEntry queryLock(String lockKey) {
        LockEntry entry = lockStore.get(lockKey);
        if (entry == null || entry.isExpired()) {
            return null;
        }
        return entry;
    }

    public ConcurrentHashMap<String, LockEntry> getLockStore() {
        return lockStore;
    }

    public long getCurrentFencingToken() {
        return fencingTokenCounter.get();
    }
}
