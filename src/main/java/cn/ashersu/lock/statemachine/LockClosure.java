package cn.ashersu.lock.statemachine;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

import java.util.concurrent.CountDownLatch;

/**
 * Raft Task 的回调，用于将状态机应用结果异步传回给 RPC 处理器。
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
