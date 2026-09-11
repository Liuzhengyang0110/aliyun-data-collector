package cn.kjky.collector.normalize;

/** 将一种采集端原始记录转换为 cc-ingest v1 标准事件。 */
@FunctionalInterface
public interface EventNormalizer<T> {
    NormalizationResult normalize(T source, NormalizationContext context);
}
