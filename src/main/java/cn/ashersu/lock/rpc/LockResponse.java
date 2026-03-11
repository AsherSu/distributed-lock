package cn.ashersu.lock.rpc;

import java.io.Serializable;

/**
 * Raft 节点处理完 {@link LockRequest} 后返回给客户端的响应消息。
 */
public class LockResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 操作是否成功。 */
    private boolean success;

    /**
     * ACQUIRE 成功时服务端返回的围栏令牌，客户端需要保存，
     * 后续 RELEASE 时原样带回。
     * 其他情况下为 -1。
     */
    private long fencingToken = -1;

    /**
     * 当前节点不是 Leader 时，通过此字段告知客户端真正的 Leader 地址（ip:port），
     * 客户端收到后应重定向请求。格式与 jraft PeerId 的 toString() 一致。
     */
    private String leaderAddr;

    /** 失败原因描述，便于客户端日志排查。 */
    private String errorMsg;

    public LockResponse() {}

    public static LockResponse success(long fencingToken) {
        LockResponse r = new LockResponse();
        r.success = true;
        r.fencingToken = fencingToken;
        return r;
    }

    public static LockResponse fail(String errorMsg) {
        LockResponse r = new LockResponse();
        r.success = false;
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

    /** 是否需要重定向到 Leader。 */
    public boolean isRedirect() { return leaderAddr != null && !leaderAddr.isEmpty(); }
}
