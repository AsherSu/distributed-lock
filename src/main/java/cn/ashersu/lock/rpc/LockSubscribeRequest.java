package cn.ashersu.lock.rpc;

import cn.ashersu.lock.statemachine.LockType;

import java.io.Serializable;

/**
 * client 订阅锁释放通知的 RPC 请求。
 */
public class LockSubscribeRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private LockType lockType;

    private String lockKey;

    private String clientId;

    private String threadId;

    public LockSubscribeRequest() {}

    public LockSubscribeRequest(String lockKey, String clientId) {
        this.lockKey = lockKey;
        this.clientId = clientId;
    }

    public LockType getLockType() {
        return lockType;
    }

    public String getLockKey() { return lockKey; }
    public void setLockKey(String lockKey) { this.lockKey = lockKey; }

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public String getThreadId() {
        return threadId;
    }
}
