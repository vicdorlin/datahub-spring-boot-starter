package tech.jmeng.datahub.spring.boot.datahub.vo;

import lombok.*;
import tech.jmeng.datahub.spring.boot.datahub.DataHubEmbedLogic;

/**
 * datahub作业参数
 *
 * @author linx 2020-10-12 1:49 下午
 */
@Setter
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataHubJobParamVo {
    private String project;
    private String topic;
    private String subId;
    private DataHubEmbedLogic embedLogic;
}
