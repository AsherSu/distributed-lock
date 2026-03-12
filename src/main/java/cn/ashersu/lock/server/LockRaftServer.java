package cn.ashersu.lock.server;

import cn.ashersu.lock.lock.LockNotifier;
import cn.ashersu.lock.statemachine.LockStateMachine;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Raft 节点的启动与生命周期管理。
 *
 * <h3>启动流程</h3>
 * <pre>
 *   LockRaftServer server = new LockRaftServer();
 *   server.start("/data/raft/node1", "lock-group",
 *                PeerId.parsePeer("127.0.0.1:8081"),
 *                new Configuration(Arrays.asList(
 *                    PeerId.parsePeer("127.0.0.1:8081"),
 *                    PeerId.parsePeer("127.0.0.1:8082"),
 *                    PeerId.parsePeer("127.0.0.1:8083"))));
 * </pre>
 *
 * <h3>jraft 核心概念速查</h3>
 * <ul>
 *   <li>{@link NodeOptions}：配置选项，包含日志路径、快照路径、选举超时、心跳间隔等。</li>
 *   <li>{@link RaftGroupService}：管理一个 Raft 组（由 groupId + PeerId + NodeOptions 确定）的服务。</li>
 *   <li>{@link Node}：代表本地 Raft 节点，提供 {@code apply(Task)}、{@code getLeaderId()} 等 API。</li>
 *   <li>{@link RpcServer}：底层 Bolt RPC 服务，由 {@link RaftRpcServerFactory} 创建，
 *       需要在 start 之前注册所有 {@code RpcProcessor}。</li>
 * </ul>
 */
public class LockRaftServer {

    private static final Logger LOG = LoggerFactory.getLogger(LockRaftServer.class);

    private RaftGroupService raftGroupService;
    private Node node;
    private LockStateMachine stateMachine;
    private LockNotifier notifier;

    /**
     * 启动 Raft 节点。
     *
     * @param dataPath  数据根目录（程序会在此目录下自动创建 log / meta / snapshot 子目录）
     * @param groupId   Raft 组 ID，集群内所有节点必须相同，例如 "lock-group"
     * @param serverId  本节点地址，格式 "ip:port"，例如 PeerId.parsePeer("127.0.0.1:8081")
     * @param initConf  初始集群配置（仅首次启动时生效，后续以持久化的配置为准）
     *
     */
    public void start(String dataPath, String groupId, PeerId serverId, Configuration initConf) throws IOException {
        this.stateMachine = new LockStateMachine();
        this.notifier = new LockNotifier();
        NodeOptions opts = new NodeOptions();
        opts.setFsm(stateMachine);                // 注册状态机
        opts.setLogUri(dataPath + "\\log");       // Raft 日志路径（RocksDB）
        opts.setRaftMetaUri(dataPath + "\\meta"); // 元数据路径（term、voteFor 等）
        opts.setSnapshotUri(dataPath + "\\snapshot"); // 快照路径
        opts.setInitialConf(initConf);           // 初始成员配置
        opts.setElectionTimeoutMs(5000);         // 选举超时（建议 3~10 倍心跳间隔）
        opts.setSnapshotIntervalSecs(30);        // 快照间隔（秒）
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        rpcServer.registerProcessor(new LockServiceProcessor(this, notifier));
        rpcServer.registerProcessor(new LockSubscribeProcessor(this, notifier));
        this.raftGroupService = new RaftGroupService(groupId, serverId, opts, rpcServer);
        this.node = raftGroupService.start();
    }

    /**
     * 优雅关闭 Raft 节点。
     * 调用 {@code raftGroupService.shutdown()} 并等待其完成。
     */
    public void shutdown() {
        raftGroupService.shutdown();
    }

    /**
     * 获取 Raft Node 实例，供 {@link LockServiceProcessor} 提交 Task 和查询 Leader。
     */
    public Node getNode() {
        return node;
    }

    public LockStateMachine getStateMachine() {
        return stateMachine;
    }

    public static void main(String[] args) {
        LockRaftServer server = new LockRaftServer();
        try {
            server.start(Paths.get("E:\\project\\distributed-lock","data", "raft", "node1").toString(),
                    "lock-group",
                    PeerId.parsePeer("127.0.0.1:8081"),
                    new Configuration(Arrays.asList(
                            PeerId.parsePeer("127.0.0.1:8081"),
                            PeerId.parsePeer("127.0.0.1:8082"),
                            PeerId.parsePeer("127.0.0.1:8083")))
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
