package tech.jmeng.datahub.spring.boot.datahub.vo.request;

import com.aliyun.datahub.client.model.SubscriptionState;
import lombok.*;
import tech.jmeng.datahub.spring.boot.datahub.DataHubEmbedLogic;
import tech.jmeng.datahub.spring.boot.datahub.vo.BaseRequestVo;

import java.util.List;

/**
 * Topic统一处理相关参数类
 *
 * @author linx 2020-09-01 4:36 下午
 */
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Builder
public class TopicParamRequestVo extends BaseRequestVo {
    private String projectName;
    private String topicName;
    @Builder.Default
    private Integer shardCount = 10;
    private List<PlainField> fields;
    /**
     * 多少天过期
     */
    @Builder.Default
    private Integer lifeCycle = 1;
    private String topicComment;

    /**
     * 创建订阅
     */
    private String subId;
    private SubscriptionCommentVo subscriptionComment;
    private SubscriptionState state;
    /**
     * 订阅session相关记录
     */
    private String sessionId;
    /**
     * 提交点位的信息
     */
    private String shardId;
    private Long sequence;
    private Long timestamp;
    /**
     * 重置点位的时间 "yyyy-MM-dd HH:mm:ss" 如果有timestamp则以timestamp为准
     */
    private String time;

    /**
     * 活跃的shardIds
     */
    private List<String> activeShardIds;

    /**
     * 内嵌业务处理逻辑
     */
    private DataHubEmbedLogic embedLogic;
}