package cn.ashersu.lock.command;

/**
 * 写入 Raft 日志的命令类型。
 */
public enum LockCommandType {

    /**
     * 申请锁。
     */
    ACQUIRE,

    /**
     * 释放锁。
     */
    RELEASE,

    /**
     * 续期
     */
    RENEW
}
