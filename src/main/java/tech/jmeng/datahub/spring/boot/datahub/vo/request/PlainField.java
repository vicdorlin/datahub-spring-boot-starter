package tech.jmeng.datahub.spring.boot.datahub.vo.request;

import lombok.*;

/**
 * datahub普通字段配置类
 * 该类转化为datahub RecordSchema中相应field结构
 *
 * @author linx 2020-09-01 5:48 下午
 */
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Builder
public class PlainField {
    private String name;
    @Builder.Default
    private String type = "STRING";
    @Builder.Default
    private boolean allowNull = true;
}
