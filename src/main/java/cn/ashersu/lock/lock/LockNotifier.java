package cn.ashersu.lock.lock;

import cn.ashersu.lock.rpc.LockResponse;
import cn.ashersu.lock.server.LockServiceProcessor;
import com.alipay.sofa.jraft.rpc.RpcContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 服务端锁释放通知器，实现 Redisson 风格的 pub/sub 等待机制。
 *
 * <h3>原理</h3>
 * <ol>
 *   <li>客户端加锁失败后，向 Leader 发送 {@link LockSubscribeRequest}，处理器将
 *       对应的 {@link RpcContext} 存入本类的等待队列（长轮询）。</li>
 *   <li>当 Leader 的 {@link LockServiceProcessor} 确认锁被完全释放时，
 *       调用 {@link #notify(String)} 唤醒该 lockKey 下所有等待的客户端。</li>
 *   <li>客户端收到响应后，其本地 {@code Semaphore} 被释放，线程醒来并重试加锁。</li>
 * </ol>
 *
 * <h3>线程安全</h3>
 * 使用 {@link ConcurrentHashMap} + {@link ConcurrentLinkedQueue} 保证并发安全，
 * 无需额外加锁。
 */
public class LockNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(LockNotifier.class);

    /**
     * key = lockKey，value = 订阅该锁释放事件的客户端 RpcContext 队列。
     * 每个 RpcContext 对应一个长轮询中的客户端连接。
     */
    private final ConcurrentHashMap<String, Queue<RpcContext>> waiters = new ConcurrentHashMap<>();

    /**
     * 注册一个客户端对指定锁的订阅。
     * 调用方不应立即回复该 ctx，而是将其存入此处，等待 {@link #notify} 触发回复。
     *
     * @param lockKey 锁名称
     * @param ctx     bolt RpcContext，持有该请求的连接与回复能力
     */
    public void subscribe(String lockKey, RpcContext ctx) {
        waiters.computeIfAbsent(lockKey, k -> new ConcurrentLinkedQueue<>()).add(ctx);
        LOG.debug("Client subscribed for lock release: {}", lockKey);
    }

    /**
     * 锁被完全释放时调用，唤醒所有等待该锁的客户端。
     * 向每个等待的 RpcContext 发送一个空的成功响应，触发客户端端的回调。
     *
     * @param lockKey 已释放的锁名称
     */
    public void notify(LockType lockType,String lockKey) {
        Queue<RpcContext> ctxQueue = waiters.remove(lockKey);
        if (ctxQueue == null || ctxQueue.isEmpty()) {
            return;
        }
        LockResponse notification = LockResponse.success(lockType,-1);
        for (RpcContext ctx : ctxQueue) {
            try {
                ctx.sendResponse(notification);
            } catch (Exception e) {
                // 客户端已断开或请求已超时，忽略
                LOG.debug("Failed to notify subscriber for lock {}: {}", lockKey, e.getMessage());
            }
        }
        LOG.debug("Notified {} waiter(s) for lock: {}", ctxQueue.size(), lockKey);
    }
}
