package cn.ashersu.lock.client;

import cn.ashersu.lock.command.LockCommandType;
import cn.ashersu.lock.rpc.LockRequest;
import cn.ashersu.lock.rpc.LockResponse;
import cn.ashersu.lock.rpc.LockSubscribeRequest;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

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

    /** jraft 提供的 RPC 客户端服务。*/
    private CliClientServiceImpl rpcClient;

    // 客户端唯一标识
    private final String clientId;

    /** RPC 超时（毫秒）。 */
    private static final int RPC_TIMEOUT_MS = 5_000;

    /** Leader 发现失败时的最大重试次数。 */
    private static final int MAX_RETRY = 3;

    public DistributedLockClient(List<PeerId> knownPeers) {
        // 初始化 rpc
        CliClientServiceImpl cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());
        this.rpcClient = cliClientService;
        // 配置 raft 集群
        this.knownPeers = knownPeers;
        // 生成客户端id
        this.clientId = buildClientId();
    }

    /**
     * 阻塞式加锁，直到成功获取锁为止（可被中断）。
     *
     * <h3>实现策略（Redisson 风格）</h3>
     * <ol>
     *   <li>调用 {@link #tryLock} 尝试一次。</li>
     *   <li>若失败，从响应中读取锁的剩余 TTL，向 Leader 发送订阅请求（长轮询）。</li>
     *   <li>本地通过 {@link Semaphore#tryAcquire} 挂起线程，最多等待 TTL 毫秒。</li>
     *   <li>当 Leader 通知锁已释放（或等待超时），Semaphore 被释放，线程醒来重试。</li>
     * </ol>
     *
     * @param lockKey  资源名称
     * @param threadId 当前线程标识（用于可重入判断）
     * @param ttlMs    每次成功加锁后的过期时间（毫秒）
     * @return 成功加锁后的 {@link LockResponse}（含 fencingToken）
     * @throws InterruptedException 若线程在等待过程中被中断
     */
    public LockResponse lock(String lockKey, String threadId, long ttlMs) throws InterruptedException {
        while (true) {
            LockResponse response = tryLock(lockKey, threadId, ttlMs);
            if (response.isSuccess()) {
                return response;
            }

            // 取锁的剩余 TTL 作为订阅等待的上限；若未携带则使用请求的 ttlMs
            long waitMs = response.getRemainingTtlMs() > 0 ? response.getRemainingTtlMs() : ttlMs;

            // Semaphore 初始计数为 0，线程将在 tryAcquire 处挂起
            Semaphore semaphore = new Semaphore(0);
            subscribeAsync(lockKey, semaphore, waitMs);

            // 挂起，直到：① 服务端推送锁释放通知；② 等待超时；③ 线程被中断
            semaphore.tryAcquire(waitMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 向 Leader 异步发送订阅请求（长轮询），通过守护线程实现非阻塞等待。
     *
     * <p>守护线程调用 {@code invokeSync} 阻塞等待服务端响应。
     * 服务端在锁释放时才回复，触发 {@code semaphore.release()}，唤醒调用方线程。
     * 若网络异常或超时，同样释放信号量，使调用方能够重试。
     */
    private void subscribeAsync(String lockKey, Semaphore semaphore, long timeoutMs) {
        if (currentLeader == null) {
            semaphore.release();
            return;
        }
        final PeerId leader = currentLeader;
        final RpcClient rpc = rpcClient.getRpcClient();
        final LockSubscribeRequest req = new LockSubscribeRequest(lockKey, clientId);
        Thread t = new Thread(() -> {
            try {
                rpc.invokeSync(leader.getEndpoint(), req, null, timeoutMs);
            } catch (Exception e) {
                LOG.warn("Subscribe for lock '{}' ended: {}", lockKey, e.getMessage());
                currentLeader = null;
            } finally {
                semaphore.release();
            }
        }, "lock-sub-" + lockKey);
        t.setDaemon(true);
        t.start();
    }

    /**
     * 尝试申请锁，非阻塞（立即返回成功或失败，不排队等待）。
     */
    public LockResponse tryLock(String lockKey, String threadId,long ttlMs) {
        String requestId = UUID.randomUUID().toString();
        LockRequest request = new LockRequest();
        request.setRequestId(requestId);
        request.setType(LockCommandType.ACQUIRE);
        request.setLockKey(lockKey);
        request.setClientId(clientId);
        request.setThreadId(threadId);
        request.setTtlMs(ttlMs);
        return sendWithRedirect(request);
    }

    /**
     * 释放锁。
     *
     * @param lockKey      资源名称
     * @param fencingToken 申请锁时服务端返回的围栏令牌，必须原样传入
     * @return true 表示释放成功（或锁已不存在），false 表示校验失败
     */
    public boolean unlock(String lockKey, String threadId,long fencingToken) {
        String requestId = UUID.randomUUID().toString();
        LockRequest request = new LockRequest();
        request.setRequestId(requestId);
        request.setType(LockCommandType.RELEASE);
        request.setLockKey(lockKey);
        request.setClientId(clientId);
        request.setThreadId(threadId);
        request.setFencingToken(fencingToken);
        LockResponse lockResponse = sendWithRedirect(request);
        return lockResponse.isSuccess();
    }

    /**
     * 续期（由 {@link LockWatchdog} 调用，业务层一般不直接调用）。
     *
     * @return true 表示续期成功
     */
    public boolean renew(String lockKey, String threadId, long fencingToken, long ttlMs) {
        String requestId = UUID.randomUUID().toString();
        LockRequest request = new LockRequest();
        request.setRequestId(requestId);
        request.setType(LockCommandType.RENEW);
        request.setLockKey(lockKey);
        request.setClientId(clientId);
        request.setThreadId(threadId);
        request.setFencingToken(fencingToken);
        request.setTtlMs(ttlMs);
        LockResponse lockResponse = sendWithRedirect(request);
        return lockResponse.isSuccess();
    }

    /**
     * 发送请求并自动处理 Leader 重定向。 节点代理
     */
    private LockResponse sendWithRedirect(LockRequest request) {
        RpcClient rpc = rpcClient.getRpcClient();
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            if (currentLeader == null) {
                discoverLeader();
            }
            try {
                Object resp = rpc.invokeSync(currentLeader.getEndpoint(), request, null, RPC_TIMEOUT_MS);
                if (!(resp instanceof LockResponse)) {
                    throw new IllegalStateException("Unexpected response type: " + (resp == null ? "null" : resp.getClass().getName()));
                }
                LockResponse response = (LockResponse) resp;
                if (response.isRedirect()) {
                    String leaderAddr = response.getLeaderAddr();
                    PeerId newLeader = new PeerId();
                    if (leaderAddr != null && newLeader.parse(leaderAddr)) {
                        LOG.info("Redirected to leader: {}", newLeader);
                        currentLeader = newLeader;
                    } else {
                        currentLeader = null;
                    }
                    continue;
                }
                return response;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("RPC interrupted", e);
            } catch (Exception e) {
                LOG.warn("RPC to {} failed (attempt {}/{}): {}", currentLeader, attempt + 1, MAX_RETRY, e.getMessage());
                currentLeader = null;
            }
        }
        throw new IllegalStateException("Failed to send request after " + MAX_RETRY + " retries");
    }

    /**
     * 发现集群 Leader。
     */
    private void discoverLeader() {
        RpcClient rpc = rpcClient.getRpcClient();
        for (PeerId peer : knownPeers) {
            try {
                LockRequest probe = new LockRequest();
                probe.setType(LockCommandType.ACQUIRE);
                probe.setLockKey("__discover_probe__");
                probe.setRequestId(UUID.randomUUID().toString());
                probe.setClientId(clientId);
                probe.setThreadId(Thread.currentThread().getName());
                probe.setTtlMs(1);

                Object resp = rpc.invokeSync(peer.getEndpoint(), probe, null, RPC_TIMEOUT_MS);
                if (!(resp instanceof LockResponse)) {
                    continue;
                }
                LockResponse response = (LockResponse) resp;
                if (response.isRedirect() && response.getLeaderAddr() != null) {
                    PeerId leader = new PeerId();
                    if (leader.parse(response.getLeaderAddr())) {
                        currentLeader = leader;
                        LOG.info("Discovered leader via redirect from {}: {}", peer, leader);
                        return;
                    }
                } else {
                    currentLeader = peer;
                    LOG.info("Discovered leader directly: {}", peer);
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Leader discovery interrupted", e);
            } catch (Exception e) {
                LOG.warn("Failed to probe peer {}: {}", peer, e.getMessage());
            }
        }
        throw new IllegalStateException("Failed to discover leader from peers: " + knownPeers);
    }

    /**
     * 本客户端的唯一标识
     */
    private String buildClientId() {
        return "client-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public void shutdown() {
        rpcClient.shutdown();
    }
}
