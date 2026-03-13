package cn.ashersu.lock.statemachine;

import cn.ashersu.lock.lock.LockType;

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

    //锁的类型
    private final LockType lockType;

    // 锁的状态
    private final Status status;

    // 防护令牌
    private final long fencingToken;

    // 锁的剩余时间
    private final long remainingTtlMs;

    // 重入次数
    private final long reentrantTimes;

    // 相关信息
    private final String message;

    private LockResult(LockType lockType,Status status, long fencingToken, String message, long remainingTtlMs, long reentrantTimes) {
        this.lockType = lockType;
        this.status =status;
        this.fencingToken = fencingToken;
        this.message = message;
        this.remainingTtlMs = remainingTtlMs;
        this.reentrantTimes = reentrantTimes;
    }

    public static LockResult success(LockType lockType,long fencingToken) {
        return new LockResult(lockType,Status.SUCCESS, fencingToken, null, 0,-1);
    }

    public static LockResult locked(LockType lockType,String message, long remainingTtlMs) {
        return new LockResult(lockType,Status.LOCKED, -1, message, remainingTtlMs,-1);
    }

    public static LockResult locked(LockType lockType,String message, long remainingTtlMs,long reentrantTimes) {
        return new LockResult(lockType,Status.LOCKED, -1, message, remainingTtlMs,reentrantTimes);
    }

    public static LockResult fairLocked(LockType lockType,String message) {
        return new LockResult(lockType,Status.LOCKED, -1, message,-1,-1);
    }

    public static LockResult locked(LockType lockType,String message) {
        return new LockResult(lockType,Status.LOCKED, -1, message, -1,-1);
    }

    public static LockResult stale(LockType lockType,String message) {
        return new LockResult(lockType,Status.STALE, -1, message, -1,-1);
    }

    public static LockResult error(LockType lockType,String message) {
        return new LockResult(lockType,Status.ERROR, -1, message, -1,-1);
    }

    public boolean isSuccess() { return status == Status.SUCCESS; }
    public Status getStatus() { return status; }
    public long getFencingToken() { return fencingToken; }
    public String getMessage() { return message; }
    public long getRemainingTtlMs() { return remainingTtlMs; }
    public long getReentrantTimes() {
        return reentrantTimes;
    }
    public LockType getLockType() {
        return lockType;
    }
}
