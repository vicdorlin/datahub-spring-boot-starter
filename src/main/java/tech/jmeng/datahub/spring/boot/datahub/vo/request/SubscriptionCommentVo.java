package tech.jmeng.datahub.spring.boot.datahub.vo.request;

import lombok.*;
import tech.jmeng.datahub.spring.boot.datahub.vo.BaseVo;

/**
 * 订阅说明参数类
 *
 * @author linx 2020-10-16 3:25 下午
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubscriptionCommentVo extends BaseVo {
    /**
     * 应用
     */
    private String application;
    /**
     * 描述
     */
    private String description;
}
