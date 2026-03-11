package cn.ashersu.lock.rpc;

import cn.ashersu.lock.command.LockCommandType;

import java.io.Serializable;

/**
 * 客户端通过 Bolt RPC 发给 Raft 节点的请求消息。
 *
 * <p>jraft 的 Bolt RPC 要求消息类实现 {@link Serializable}，
 * 同时需要在 {@code RpcServer} 中通过 {@code registerProcessor} 注册对应的处理器，
 * 处理器通过 {@code interest()} 方法返回此类的全限定名来关联。
 */
public class LockRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 请求类型，用于 RPC 处理器内部路由到对应逻辑。 */
    private LockCommandType type;

    /** 目标锁资源名称。 */
    private String lockKey;

    /** 客户端唯一标识，格式建议：{hostname}_{pid}_{threadId}。 */
    private String clientId;

    /** 锁的存活时长（毫秒），ACQUIRE / RENEW 时有效。 */
    private long ttlMs;

    /** 幂等键，全局唯一，防止网络重传导致锁被多次授予。 */
    private String requestId;

    /**
     * 围栏令牌，仅 RELEASE 请求时携带。
     * 值来自 ACQUIRE 成功后服务端返回的 {@link LockResponse#fencingToken}。
     */
    private long fencingToken;

    public LockRequest() {}

    public LockCommandType getType() { return type; }
    public void setType(LockCommandType type) { this.type = type; }

    public String getLockKey() { return lockKey; }
    public void setLockKey(String lockKey) { this.lockKey = lockKey; }

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public long getTtlMs() { return ttlMs; }
    public void setTtlMs(long ttlMs) { this.ttlMs = ttlMs; }

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }

    public long getFencingToken() { return fencingToken; }
    public void setFencingToken(long fencingToken) { this.fencingToken = fencingToken; }
}
