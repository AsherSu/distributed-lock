package cn.ashersu.lock.server;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.rpc.LockRequest;
import cn.ashersu.lock.rpc.LockResponse;
import cn.ashersu.lock.statemachine.LockClosure;
import cn.ashersu.lock.statemachine.LockResult;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bolt RPC 处理器：接收客户端的 {@link LockRequest}，将其转换为 Raft {@link Task} 提交。
 *
 * <h3>注册方式</h3>
 * 在 {@link LockRaftServer#start} 中通过以下方式注册：
 * <pre>
 *   rpcServer.registerProcessor(new LockServiceProcessor(server));
 * </pre>
 *
 * <h3>写路径完整链路</h3>
 * <pre>
 * Client ──RPC──► LockServiceProcessor.handleRequest()
 *                      │
 *                      ├─ 非 Leader → 返回 LockResponse.redirect(leaderAddr)
 *                      │
 *                      └─ 是 Leader → 构造 Task，提交给 node.apply(task)
 *                                          │
 *                                    Raft 日志复制（多数节点确认）
 *                                          │
 *                                    LockStateMachine.onApply()
 *                                          │
 *                                    LockClosure.run() → 唤醒 awaitResult()
 *                                          │
 *                      构造 LockResponse 返回给客户端
 * </pre>
 */
public class LockServiceProcessor implements RpcProcessor<LockRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(LockServiceProcessor.class);

    private final LockRaftServer raftServer;

    public LockServiceProcessor(LockRaftServer raftServer) {
        this.raftServer = raftServer;
    }

    /**
     * 处理客户端发来的锁请求。
     */
    @Override
    public void handleRequest(RpcContext ctx, LockRequest request) {
        Node node = raftServer.getNode();
        if (!node.isLeader()) {
            PeerId leader = node.getLeaderId();
            String leaderAddr = leader != null ? leader.toString() : null;
            ctx.sendResponse(LockResponse.redirect(leaderAddr));
            return;
        }

        // 构造锁命令
        LockCommand cmd = null;
        switch (request.getType()){
            case ACQUIRE:
                cmd = LockCommand.acquire(request.getLockKey(), request.getClientId(),request.getThreadId(), request.getTtlMs(), request.getRequestId());
                break;
            case RENEW:
                cmd = LockCommand.renew(request.getLockKey(), request.getClientId(),request.getThreadId(), request.getTtlMs());
                break;
            case RELEASE:
                cmd = LockCommand.release(request.getLockKey(), request.getClientId(),request.getThreadId(), request.getFencingToken());
                break;
        }
        LockClosure closure = new LockClosure();
        Task task = new Task();
        task.setData(LockCommand.encode(cmd));
        task.setDone(closure);
        node.apply(task);

        try {
            // 阻塞等待状态机执行完毕（或底层框架报错）
            closure.awaitResult();

            if (closure.isOk()) {
                // 达成共识
                LockResult result = closure.getResult();
                ctx.sendResponse(result);
            } else {
                // 异常处理
                Status status = closure.getStatus();
                if (status.getRaftError() == RaftError.ELEADERREMOVED) {
                    PeerId leader = node.getLeaderId();
                    ctx.sendResponse(LockResponse.redirect(leader != null ? leader.toString() : null));
                } else {
                    ctx.sendResponse(LockResponse.fail("Raft consensus error: " + status.getErrorMsg()));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ctx.sendResponse(LockResponse.fail("Server interrupted"));
        }
    }

    /**
     * 返回此处理器感兴趣的消息类型全限定名。
     * Bolt RPC 框架通过此字符串将收到的消息路由到对应处理器。
     */
    @Override
    public String interest() {
        return LockRequest.class.getName();
    }
}
