package cn.ashersu.lock.command;

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

    /**
     * 命令类型
     */
    private LockCommandType type;

    /**
     * 被锁定的资源名称，例如 "order:pay:1001"
     */
    private String lockKey;

    /**
     * 客户端标识
     */
    private String clientId;

    /**
     * 线程标识
     */
    private String threadId;

    /**
     * 锁的存活时长（毫秒）。
     * ACQUIRE / RENEW 时有效；RELEASE 时忽略。
     */
    private long ttlMs;

    /**
     * 全局唯一请求 ID，用于实现幂等性。
     * 状态机应记录已处理的 requestId，重复请求直接返回上次结果。
     * 建议用 UUID 或 Snowflake ID 生成。
     */
    private String requestId;

    /**
     * 围栏令牌（Fencing Token），仅 RELEASE 时需要携带。
     * 状态机在 ACQUIRE 成功时将当前令牌值写入 LockEntry，
     * RELEASE 时必须与 LockEntry 中的值完全匹配才能释放。
     * 防止：GC Pause 后旧客户端用过期令牌释放新持有者的锁。
     */
    private long fencingToken;

    public LockCommand() {
    }

    public static LockCommand acquire(String lockKey, String clientId,String threadId, long ttlMs, String requestId) {
        LockCommand cmd = new LockCommand();
        cmd.type = LockCommandType.ACQUIRE;
        cmd.lockKey = lockKey;
        cmd.clientId = clientId;
        cmd.ttlMs = ttlMs;
        cmd.requestId = requestId;
        cmd.threadId = threadId;
        return cmd;
    }

    public static LockCommand release(String lockKey, String clientId,String threadId, long fencingToken) {
        LockCommand cmd = new LockCommand();
        cmd.type = LockCommandType.RELEASE;
        cmd.lockKey = lockKey;
        cmd.clientId = clientId;
        cmd.fencingToken = fencingToken;
        cmd.threadId = threadId;
        return cmd;
    }

    public static LockCommand renew(String lockKey, String clientId,String threadId, long ttlMs) {
        LockCommand cmd = new LockCommand();
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
}
