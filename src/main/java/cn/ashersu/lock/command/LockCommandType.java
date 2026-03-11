package cn.ashersu.lock.command;

/**
 * 写入 Raft 日志的命令类型。
 *
 * <p>所有修改锁状态的操作都必须通过 Raft 日志复制，状态机严格按日志顺序应用，
 * 保证集群内所有节点的锁状态最终一致。
 */
public enum LockCommandType {

    /**
     * 申请锁。
     * 状态机处理时需判断：
     *   1. 锁不存在 → 直接授予；
     *   2. 锁存在但已过期（懒清理）→ 覆盖授予；
     *   3. 锁存在且未过期 → 拒绝，返回 LOCKED。
     */
    ACQUIRE,

    /**
     * 释放锁。
     * 状态机处理时需校验 ownerId + fencingToken 双重匹配，
     * 防止过期客户端误释放他人已持有的锁。
     */
    RELEASE,

    /**
     * 续期（由 LockWatchdog 定期发起）。
     * 状态机处理时只需校验 ownerId，匹配则刷新 expireTime。
     */
    RENEW
}
