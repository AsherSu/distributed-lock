package cn.ashersu.lock.command;

import cn.ashersu.lock.lock.LockType;
import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * 写入 Raft 日志的命令体，需要能被序列化为字节数组后存入 {@code Task#setData(ByteBuffer)}。
 *
 * <h3>序列化方案选择</h3>
 * 骨架中预留了 {@link #encode} / {@link #decode} 两个静态方法，
 * 你可以选择：
 * <ul>
 *   <li>Hessian（已引入依赖，紧凑、跨语言）</li>
 *   <li>Java 原生序列化（简单，但体积大）</li>
 *   <li>手写 ByteBuffer 协议（最轻量，推荐学完后挑战）</li>
 * </ul>
 */
public class LockCommand implements Serializable {

    private static final long serialVersionUID = 1L;

    // 全局唯一请求 ID，用于实现幂等性。
    private String requestId;

    // 命令类型
    private LockCommandType type;

    // 锁类型
    private LockType lockType;

    // 被锁定的资源名称
    private String lockKey;

    // 确保锁的归属
    private String clientId;
    private String threadId;

    // 锁的存活时长（毫秒）。
    private long ttlMs;

    // 围栏令牌（Fencing Token），利用递增的特性，保护下游资源。防止stw导致锁过期
    private long fencingToken;

    public LockCommand() {
    }

    public static LockCommand acquire(LockType lockType,String lockKey, String clientId,String threadId, long ttlMs, String requestId) {
        LockCommand cmd = new LockCommand();
        cmd.lockType = lockType;
        cmd.type = LockCommandType.ACQUIRE;
        cmd.lockKey = lockKey;
        cmd.clientId = clientId;
        cmd.ttlMs = ttlMs;
        cmd.requestId = requestId;
        cmd.threadId = threadId;
        return cmd;
    }

    public static LockCommand release(LockType lockType,String lockKey, String clientId,String threadId, long fencingToken) {
        LockCommand cmd = new LockCommand();
        cmd.lockType = lockType;
        cmd.type = LockCommandType.RELEASE;
        cmd.lockKey = lockKey;
        cmd.clientId = clientId;
        cmd.fencingToken = fencingToken;
        cmd.threadId = threadId;
        return cmd;
    }

    public static LockCommand renew(LockType lockType,String lockKey, String clientId,String threadId, long ttlMs) {
        LockCommand cmd = new LockCommand();
        cmd.lockType = lockType;
        cmd.type = LockCommandType.RENEW;
        cmd.lockKey = lockKey;
        cmd.clientId = clientId;
        cmd.ttlMs = ttlMs;
        cmd.threadId = threadId;
        return cmd;
    }

    /**
     * 将 LockCommand 编码为字节数组，供写入 {@code Task#setData(ByteBuffer)}。
     *
     * <p>实现提示（以 Hessian 为例）：
     * <pre>
     *   ByteArrayOutputStream bos = new ByteArrayOutputStream();
     *   HessianOutput ho = new HessianOutput(bos);
     *   ho.writeObject(command);
     *   ho.flush();
     *   return ByteBuffer.wrap(bos.toByteArray());
     * </pre>
     */
    public static ByteBuffer encode(LockCommand command) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HessianOutput ho = new HessianOutput(bos);
        try {
            ho.writeObject(command);
            ho.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ByteBuffer.wrap(bos.toByteArray());
    }

    /**
     * 从字节数组反序列化出 LockCommand，在状态机 {@code onApply} 中调用。
     *
     * <p>实现提示（以 Hessian 为例）：
     * <pre>
     *   HessianInput hi = new HessianInput(new ByteArrayInputStream(bytes));
     *   return (LockCommand) hi.readObject();
     * </pre>
     */
    public static LockCommand decode(byte[] bytes) {
        HessianInput hi = new HessianInput(new ByteArrayInputStream(bytes));
        try {
            return (LockCommand) hi.readObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // -------------------------------------------------------------------------
    // Getters / Setters
    // -------------------------------------------------------------------------

    public LockCommandType getType() {
        return type;
    }

    public String getLockKey() {
        return lockKey;
    }

    public String getClientId() {
        return clientId;
    }

    public long getTtlMs() {
        return ttlMs;
    }

    public String getRequestId() {
        return requestId;
    }

    public long getFencingToken() {
        return fencingToken;
    }

    public String getThreadId() {
        return threadId;
    }

    public void setThreadId(String threadId) {
        this.threadId = threadId;
    }

    public String getClientIdentify() {
        return clientId+":"+threadId;
    }

    public LockType getLockType() {
        return lockType;
    }

    public void setLockType(LockType lockType) {
        this.lockType = lockType;
    }
}
