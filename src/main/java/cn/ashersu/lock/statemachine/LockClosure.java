package cn.ashersu.lock.statemachine;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

import java.util.concurrent.CountDownLatch;

/**
 * Raft Task 的回调，用于将状态机应用结果异步传回给 RPC 处理器。
 *
 * <h3>工作原理</h3>
 * <ol>
 *   <li>RPC 处理器构造 LockClosure，将其设置到 {@code Task#setDone(closure)}。</li>
 *   <li>Task 提交给 Raft 后，RPC 处理器在 {@link #awaitResult()} 上阻塞等待。</li>
 *   <li>当日志被多数节点确认并应用到状态机后，jraft 在状态机线程调用 {@link #run(Status)}。</li>
 *   <li>{@link #run} 设置结果并释放 CountDownLatch，唤醒等待的 RPC 处理器。</li>
 *   <li>RPC 处理器根据 result 构造 {@code LockResponse} 返回给客户端。</li>
 * </ol>
 *
 * <p><strong>注意</strong>：如果当前节点不是 Leader，Task 提交失败时 jraft 会用
 * {@code Status(RaftError.ENOT_LEADER, ...)} 调用 {@link #run}，
 * 此时 result 为 null，处理器应检查 status 并返回重定向响应。
 */
public class LockClosure implements Closure {

    private final CountDownLatch latch = new CountDownLatch(1);

    /** 状态机应用的结果，run() 被调用前为 null。 */
    private LockResult result;

    /** Raft 操作的状态，OK 表示日志成功提交并应用。 */
    private Status status;

    @Override
    public void run(Status status) {
        this.status = status;
        this.latch.countDown();
    }

    /**
     * 阻塞等待状态机应用完成。
     * RPC 处理器在提交 Task 后调用此方法等待结果。
     *
     * @throws InterruptedException 若等待期间线程被中断
     */
    public void awaitResult() throws InterruptedException {
        this.latch.await();
    }

    public void setResult(LockResult result) {
        this.result = result;
    }

    public LockResult getResult() {
        return result;
    }

    public Status getStatus() {
        return status;
    }

    public boolean isOk() {
        return status != null && status.isOk();
    }
}
