package cn.kjky.collector.pipeline;

import cn.kjky.collector.contract.StandardEvent;

import java.util.List;

/** 接收标准事件并在单个任务窗口结束时可靠完成持久化的生产采集出口。 */
public interface CollectionEventSink {
    /** 打开一个“任务 + 查询窗口”会话。只有会话完成后才能推进该任务水位。 */
    TaskSession open(CollectionTask task) throws Exception;

    interface TaskSession {
        /** 接收一条已经通过字段准入和脱敏处理的标准事件。 */
        void accept(StandardEvent event) throws Exception;

        /** 接收一组标准事件。 */
        default void acceptAll(List<StandardEvent> events) throws Exception {
            if (events == null) return;
            for (StandardEvent event : events) accept(event);
        }

        /** 发布尾批并推进采集水位。此前任一异常均不得调用该方法。 */
        void finish() throws Exception;
    }
}
