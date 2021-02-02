package tech.jmeng.datahub.spring.boot.datahub.vo.request;

import lombok.*;
import tech.jmeng.datahub.spring.boot.datahub.vo.BaseRequestVo;

/**
 * shard相关功能处理参数类
 * 创建shard 分裂shard等操作
 *
 * @author linx 2020-09-02 2:25 下午
 */
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Builder
public class ShardParamRequestVo extends BaseRequestVo {
    private String projectName;
    private String topicName;
    private String shardId;
}
