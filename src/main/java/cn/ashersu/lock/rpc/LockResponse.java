package cn.ashersu.lock.rpc;

import cn.ashersu.lock.lock.LockType;

import java.io.Serializable;

/**
 * Raft 节点处理完 {@link LockRequest} 后返回给客户端的响应消息。
 */
public class LockResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 操作是否成功。 */
    private boolean success;

    private LockType lockType;

    /** 防护token */
    private long fencingToken = -1;

    /** leader地址 */
    private String leaderAddr;

    /** 锁的有效时间 */
    private long remainingTtlMs;

    /** 失败原因描述，便于客户端日志排查。 */
    private String errorMsg;

    public LockResponse() {}

    public static LockResponse success(LockType lockType,long fencingToken) {
        LockResponse r = new LockResponse();
        r.success = true;
        r.fencingToken = fencingToken;
        r.lockType=lockType;
        return r;
    }

    public static LockResponse fail(LockType lockType,String errorMsg) {
        LockResponse r = new LockResponse();
        r.success = false;
        r.lockType=lockType;
        r.errorMsg = errorMsg;
        return r;
    }

    /** 非 Leader 节点调用此方法，通知客户端重定向。 */
    public static LockResponse redirect(String leaderAddr) {
        LockResponse r = new LockResponse();
        r.success = false;
        r.leaderAddr = leaderAddr;
        r.errorMsg = "NOT_LEADER";
        return r;
    }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public long getFencingToken() { return fencingToken; }
    public void setFencingToken(long fencingToken) { this.fencingToken = fencingToken; }

    public String getLeaderAddr() { return leaderAddr; }
    public void setLeaderAddr(String leaderAddr) { this.leaderAddr = leaderAddr; }

    public String getErrorMsg() { return errorMsg; }
    public void setErrorMsg(String errorMsg) { this.errorMsg = errorMsg; }

    public long getRemainingTtlMs() { return remainingTtlMs; }
    public void setRemainingTtlMs(long remainingTtlMs) { this.remainingTtlMs = remainingTtlMs; }

    /** 是否需要重定向到 Leader。 */
    public boolean isRedirect() { return leaderAddr != null && !leaderAddr.isEmpty(); }
}
