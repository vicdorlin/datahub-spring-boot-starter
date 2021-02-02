package tech.jmeng.datahub.spring.boot.datahub.vo.request;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * topic写入属性参数类
 *
 * @author linx 2020-09-02 2:25 下午
 */
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class Write2TupleTopicParamRequestVo extends ShardParamRequestVo {
    private List<Write2TopicAttribute> attributes;
}
