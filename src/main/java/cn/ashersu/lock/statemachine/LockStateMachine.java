package cn.ashersu.lock.statemachine;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.command.LockCommandType;
import cn.ashersu.lock.lock.LockProcessor;
import cn.ashersu.lock.lock.LockRouter;
import cn.ashersu.lock.lock.MutexLockProcessor;
import cn.ashersu.lock.rpc.LockResponse;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 分布式锁状态机。日志管理
 */
public class LockStateMachine extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(LockStateMachine.class);

    // 分布式锁处理路由
    private final LockRouter router = new LockRouter();

    {
        router.registerProcessor(new MutexLockProcessor());
    }

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
            LockResult lockResult = router.routeAndProcess(cmd);
            if (done != null) {
                done.setResult(lockResult);
                done.run(Status.OK());
            }
            iter.next();

        }
    }

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

    }

    /**
     * 加载快照：节点启动或落后太多时，从磁盘快照恢复状态。
     */
    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        return true;
    }

//    // -------------------------------------------------------------------------
//    // 读路径：线性一致读（查询锁状态，可选实现）
//    // -------------------------------------------------------------------------
//
//    /**
//     * 查询指定 lockKey 当前的持锁状态（供 ReadIndex 读使用）。
//     *
//     * <p>此方法本身不走 Raft 日志，但调用前需要通过
//     * {@code node.readIndex(requestContext, readIndexClosure)} 确认当前节点仍是 Leader
//     * 且已应用到最新的 commitIndex，才能保证读到的是线性一致的结果。
//     *
//     * @return null 表示锁不存在或已过期
//     */
//    public LockEntry queryLock(String lockKey) {
//        LockEntry entry = lockStore.get(lockKey);
//        if (entry == null || entry.isExpired()) {
//            return null;
//        }
//        return entry;
//    }
}
