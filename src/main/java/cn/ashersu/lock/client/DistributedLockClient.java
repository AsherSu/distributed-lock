package cn.ashersu.lock.client;

import cn.ashersu.lock.rpc.LockRequest;
import cn.ashersu.lock.rpc.LockResponse;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/**
 * 分布式锁客户端，对外暴露 {@link #tryLock} / {@link #unlock} / {@link #renew} 接口。
 *
 * <h3>Leader 发现策略</h3>
 * <ol>
 *   <li>启动时轮询 {@link #knownPeers} 中的任意节点，若返回 redirect 响应则更新 {@link #currentLeader}。</li>
 *   <li>后续请求直接发给 currentLeader，避免每次都重定向。</li>
 *   <li>若 currentLeader 请求失败（Leader 换届），重新从 knownPeers 发现新 Leader。</li>
 * </ol>
 *
 * <h3>幂等重试</h3>
 * <p>每次 tryLock 生成唯一 requestId（UUID），重试时携带同一个 requestId，
 * 即使请求被服务端重复处理，状态机也会返回第一次的结果（需在 applyAcquire 中实现幂等逻辑）。
 */
public class DistributedLockClient {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedLockClient.class);

    /** 已知的集群节点列表，用于 Leader 发现（不要求全部在线）。 */
    private final List<PeerId> knownPeers;

    /** 当前已知的 Leader 节点，初始为 null，首次请求时通过重定向发现。 */
    private volatile PeerId currentLeader;

    /**
     * jraft 提供的 Bolt RPC 客户端服务。
     * 使用 {@link BoltCliClientService} 包装，可直接向指定 PeerId 发送任意消息。
     *
     * <p>初始化：
     * <pre>
     *   BoltCliClientService clientService = new BoltCliClientService();
     *   clientService.init(new CliOptions());
     *   this.rpcClient = clientService.getRpcClient();
     * </pre>
     */
    private RpcClient rpcClient;

    /** RPC 超时（毫秒）。 */
    private static final int RPC_TIMEOUT_MS = 5_000;

    /** Leader 发现失败时的最大重试次数。 */
    private static final int MAX_RETRY = 3;

    public DistributedLockClient(List<PeerId> knownPeers) {
        this.knownPeers = knownPeers;
        // TODO: 初始化 rpcClient（见字段注释）
    }

    /**
     * 尝试申请锁，非阻塞（立即返回成功或失败，不排队等待）。
     *
     * @param lockKey 资源名称
     * @param ttlMs   锁的存活时长（毫秒），建议结合业务预计耗时 + 足够续期次数设定
     * @return 申请结果，成功时可从中取到 fencingToken；失败时 isSuccess() == false
     *
     * <h3>实现步骤</h3>
     * <ol>
     *   <li>生成 requestId = UUID.randomUUID().toString()。</li>
     *   <li>构造 LockRequest（type=ACQUIRE, lockKey, clientId, ttlMs, requestId）。</li>
     *   <li>调用 {@link #sendWithRedirect(LockRequest)} 发送请求（内部处理 Leader 重定向）。</li>
     *   <li>返回 LockResponse。</li>
     * </ol>
     */
    public LockResponse tryLock(String lockKey, long ttlMs) {
        // TODO: 实现 tryLock（见上方 Javadoc）
        throw new UnsupportedOperationException("TODO: 实现 tryLock");
    }

    /**
     * 释放锁。
     *
     * @param lockKey      资源名称
     * @param fencingToken 申请锁时服务端返回的围栏令牌，必须原样传入
     * @return true 表示释放成功（或锁已不存在），false 表示校验失败
     *
     * <h3>实现步骤</h3>
     * <ol>
     *   <li>构造 LockRequest（type=RELEASE, lockKey, clientId, fencingToken）。</li>
     *   <li>调用 {@link #sendWithRedirect(LockRequest)} 发送请求。</li>
     *   <li>返回 response.isSuccess()。</li>
     * </ol>
     */
    public boolean unlock(String lockKey, long fencingToken) {
        // TODO: 实现 unlock（见上方 Javadoc）
        throw new UnsupportedOperationException("TODO: 实现 unlock");
    }

    /**
     * 续期（由 {@link LockWatchdog} 调用，业务层一般不直接调用）。
     *
     * @return true 表示续期成功
     */
    public boolean renew(String lockKey, String clientId, long ttlMs) {
        // TODO: 实现 renew（同 tryLock 流程，type=RENEW）
        throw new UnsupportedOperationException("TODO: 实现 renew");
    }

    /**
     * 发送请求并自动处理 Leader 重定向。
     *
     * <h3>实现逻辑</h3>
     * <ol>
     *   <li>若 currentLeader 为 null，先调用 {@link #discoverLeader()}。</li>
     *   <li>向 currentLeader 发送请求（同步调用 {@code rpcClient.invokeSync}）。</li>
     *   <li>若响应 isRedirect()：
     *     <ul>
     *       <li>解析 response.getLeaderAddr() 更新 currentLeader。</li>
     *       <li>重试，最多 MAX_RETRY 次。</li>
     *     </ul>
     *   </li>
     *   <li>网络异常（TimeoutException 等）→ 将 currentLeader 置 null，重新发现后重试。</li>
     * </ol>
     */
    private LockResponse sendWithRedirect(LockRequest request) {
        // TODO: 实现带重定向的请求发送（见上方 Javadoc）
        throw new UnsupportedOperationException("TODO: 实现 sendWithRedirect");
    }

    /**
     * 发现集群 Leader。
     * 轮询 knownPeers，发送一个 ACQUIRE 或 ping 请求，
     * 从 redirect 响应中拿到 leaderAddr，更新 currentLeader。
     *
     * <p><strong>提示</strong>：也可以通过 jraft 的 CliService 查询 Leader，
     * 但直接走重定向更简单，不需要额外引入 jraft-rheakv-core。
     */
    private void discoverLeader() {
        // TODO: 实现 Leader 发现（见上方 Javadoc）
        throw new UnsupportedOperationException("TODO: 实现 discoverLeader");
    }

    /**
     * 本客户端的唯一标识，建议格式：{hostname}_{pid}_{threadId}。
     * 可在构造时通过 ManagementFactory.getRuntimeMXBean().getName() 获取 pid。
     */
    private String buildClientId() {
        // TODO: 构造并返回客户端唯一标识
        return "client-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public void shutdown() {
        // TODO: 关闭 rpcClient
    }
}
