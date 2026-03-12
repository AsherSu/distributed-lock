package cn.ashersu.lock.server;

import cn.ashersu.lock.rpc.LockResponse;
import cn.ashersu.lock.rpc.LockSubscribeRequest;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理client的 锁释放 订阅请求（长轮询）。
 */
public class LockSubscribeProcessor implements RpcProcessor<LockSubscribeRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(LockSubscribeProcessor.class);

    private final LockRaftServer raftServer;
    private final LockNotifier notifier;

    public LockSubscribeProcessor(LockRaftServer raftServer, LockNotifier notifier) {
        this.raftServer = raftServer;
        this.notifier = notifier;
    }

    @Override
    public void handleRequest(RpcContext ctx, LockSubscribeRequest request) {
        Node node = raftServer.getNode();
        if (!node.isLeader()) {
            PeerId leader = node.getLeaderId();
            String leaderAddr = leader != null ? leader.toString() : null;
            ctx.sendResponse(LockResponse.redirect(leaderAddr));
            return;
        }
        // 长轮询：将 ctx 挂起，等待 LockNotifier 在锁释放时触发回复
        LOG.debug("Registering subscribe for lock '{}' from client '{}'", request.getLockKey(), request.getClientId());
        notifier.subscribe(request.getLockKey(), ctx);
    }

    @Override
    public String interest() {
        return LockSubscribeRequest.class.getName();
    }
}
