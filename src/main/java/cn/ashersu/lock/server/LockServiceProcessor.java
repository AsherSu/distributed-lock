package cn.ashersu.lock.server;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.rpc.LockRequest;
import cn.ashersu.lock.rpc.LockResponse;
import cn.ashersu.lock.statemachine.LockClosure;
import cn.ashersu.lock.statemachine.LockResult;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
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
     *
     * <h3>实现步骤</h3>
     * <ol>
     *   <li>获取 Node：{@code Node node = raftServer.getNode()}。</li>
     *   <li>判断是否是 Leader：
     *     <pre>
     *     if (!node.isLeader()) {
     *         PeerId leader = node.getLeaderId();
     *         String leaderAddr = leader != null ? leader.toString() : null;
     *         ctx.sendResponse(LockResponse.redirect(leaderAddr));
     *         return;
     *     }
     *     </pre>
     *   </li>
     *   <li>将 LockRequest 转换为 LockCommand（字段一一对应）。</li>
     *   <li>序列化 LockCommand 为 ByteBuffer：{@code LockCommand.encode(cmd)}。</li>
     *   <li>构造 LockClosure，设置到 Task：
     *     <pre>
     *     LockClosure closure = new LockClosure();
     *     Task task = new Task();
     *     task.setData(LockCommand.encode(cmd));
     *     task.setDone(closure);
     *     node.apply(task);
     *     </pre>
     *   </li>
     *   <li>等待结果：{@code closure.awaitResult()}（此处阻塞直到状态机应用完毕）。</li>
     *   <li>将 {@link LockResult} 映射为 {@link LockResponse} 通过 ctx.sendResponse() 返回。</li>
     * </ol>
     *
     * <p><strong>超时建议</strong>：awaitResult() 应设置超时（使用 CountDownLatch.await(timeout, unit)），
     * 超时后返回错误响应，防止客户端永久阻塞。
     */
    @Override
    public void handleRequest(RpcContext ctx, LockRequest request) {
        // TODO: 实现请求处理（见上方 Javadoc）
        ctx.sendResponse(LockResponse.fail("TODO: 未实现 handleRequest"));
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
