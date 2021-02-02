package tech.jmeng.datahub.spring.boot.datahub.vo.request;

import lombok.*;
import tech.jmeng.datahub.spring.boot.datahub.vo.BaseVo;

import java.util.Map;

/**
 * topic写入属性
 *
 * @author linx 2020-09-02 3:47 下午
 */
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Builder
public class Write2TopicAttribute extends BaseVo {
    private Map<String, Object> field;
    private Map<String, String> addition;
}
