package cn.ashersu.lock.rpc;

import cn.ashersu.lock.command.LockCommandType;
import cn.ashersu.lock.lock.LockType;

import java.io.Serializable;

/**
 * 请求入口
 */
public class LockRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 请求类型，用于 RPC 处理器内部路由到对应逻辑。 */
    private LockCommandType type;

    private LockType lockType;

    /** 目标锁资源名称。 */
    private String lockKey;

    /** 客户端唯一标识 */
    private String clientId;

    private String threadId;

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

    public String getThreadId() {
		return threadId;
	}

    public void setThreadId(String threadId) {
		this.threadId = threadId;
	}

    public LockType getLockType() {
        return lockType;
    }

    public void setLockType(LockType lockType) {
        this.lockType = lockType;
    }
}
