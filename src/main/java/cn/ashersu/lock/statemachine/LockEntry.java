package cn.ashersu.lock.statemachine;

import java.io.Serializable;

/**
 * 状态机内存中维护的锁状态单元，保存在 {@code LockStateMachine#lockStore} 的 Map 值中。
 *
 * <p>Snapshot 保存时，整个 Map&lt;String, LockEntry&gt; 需要被序列化到磁盘。
 */
public class LockEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private LockType lockType;

    /**
     * 锁的资源名称，与 Map 的 key 相同（冗余存储，方便日志打印）。
     */
    private String lockKey;

    /**
     * 持锁方的唯一标识，等于 LockCommand#clientId:threadId。
     * 确保锁的归属唯一
     */
    private String ownerId;

    /**
     * 锁的绝对过期时间戳（System.currentTimeMillis() + ttlMs）。
     * 判断锁是否过期：System.currentTimeMillis() > expireTime。
     */
    private long expireTime;

    /**
     * 围栏令牌，全局单调递增。利用递增的特性，保护下游资源。防止stw导致锁过期
     */
    private long fencingToken;

    /**
     * 锁的重入次数
     */
    private long reentrantTimes;

    public LockEntry() {}

    public LockEntry(LockType lockType,String lockKey, String ownerId, long expireTime, long fencingToken) {
        this.lockType=lockType;
        this.lockKey = lockKey;
        this.ownerId = ownerId;
        this.expireTime = expireTime;
        this.fencingToken = fencingToken;
    }

    /** 判断当前锁是否已过期（懒清理入口）。 */
    public boolean isExpired() {
        return System.currentTimeMillis() > expireTime;
    }

    public LockType getLockType() {
        return lockType;
    }

    public void setLockType(LockType lockType) {
        this.lockType = lockType;
    }

    public String getLockKey() { return lockKey; }
    public void setLockKey(String lockKey) { this.lockKey = lockKey; }

    public String getOwnerId() { return ownerId; }
    public void setOwnerId(String ownerId) { this.ownerId = ownerId; }

    public long getExpireTime() { return expireTime; }
    public void setExpireTime(long expireTime) { this.expireTime = expireTime; }

    public long getFencingToken() { return fencingToken; }
    public void setFencingToken(long fencingToken) { this.fencingToken = fencingToken; }

    public long getReentrantTimes() {
        return reentrantTimes;
    }
    public void setReentrantTimes(long reentrantTimes) {
        this.reentrantTimes = reentrantTimes;
    }

    @Override
    public String toString() {
        return "LockEntry{" +
                "lockType=" + lockType +
                ", lockKey='" + lockKey + '\'' +
                ", ownerId='" + ownerId + '\'' +
                ", expireTime=" + expireTime +
                ", fencingToken=" + fencingToken +
                ", reentrantTimes=" + reentrantTimes +
                '}';
    }
}
