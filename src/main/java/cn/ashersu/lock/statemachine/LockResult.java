package cn.ashersu.lock.statemachine;

/**
 * 状态机应用一条 LockCommand 后的处理结果，通过 Closure 回调传回给 RPC 处理器。
 *
 * <p>注意：这是状态机内部的回调结果，不要与 RPC 层的 {@code LockResponse} 混淆。
 * RPC 处理器拿到 LockResult 后，再将其映射为 LockResponse 返回给客户端。
 */
public class LockResult {

    public enum Status {
        /** 操作成功（ACQUIRE 成功即为授予锁）。 */
        SUCCESS,
        /** 锁已被其他客户端持有且未过期。 */
        LOCKED,
        /** fencingToken 或 ownerId 校验失败（针对 RELEASE）。 */
        STALE,
        /** 状态机内部异常。 */
        ERROR
    }

    private final Status status;

    /**
     * ACQUIRE 成功时返回的围栏令牌，客户端保存后用于 RELEASE 校验。
     * 其他情况下值为 -1。
     */
    private final long fencingToken;

    /** 附加的错误或提示信息，便于调试。 */
    private final String message;

    private LockResult(Status status, long fencingToken, String message) {
        this.status = status;
        this.fencingToken = fencingToken;
        this.message = message;
    }

    public static LockResult success(long fencingToken) {
        return new LockResult(Status.SUCCESS, fencingToken, null);
    }

    public static LockResult locked(String message) {
        return new LockResult(Status.LOCKED, -1, message);
    }

    public static LockResult stale(String message) {
        return new LockResult(Status.STALE, -1, message);
    }

    public static LockResult error(String message) {
        return new LockResult(Status.ERROR, -1, message);
    }

    public boolean isSuccess() { return status == Status.SUCCESS; }
    public Status getStatus() { return status; }
    public long getFencingToken() { return fencingToken; }
    public String getMessage() { return message; }
}
