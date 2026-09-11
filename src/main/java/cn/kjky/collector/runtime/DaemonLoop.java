package cn.kjky.collector.runtime;

/** CollectorDaemon 管理的独立常驻循环。 */
public interface DaemonLoop extends AutoCloseable {
    /** 启动循环；重复调用必须保持幂等。 */
    void start();

    /** 停止接收新工作并在自身宽限期内完成优雅退出。 */
    @Override
    void close();
}
