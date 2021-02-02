package tech.jmeng.datahub.spring.boot.datahub;

import com.aliyun.datahub.client.model.RecordData;

/**
 * Datahub消费逻辑内嵌接口
 * 客户端自定义具体实现逻辑
 * 通过{@link DataHubOperator}提交作业时通过相应参数传入
 *
 * @author linx 2020-10-04 7:43 下午
 */
public interface DataHubEmbedLogic {
    /**
     * datahub实时消费数据处理逻辑
     *
     * @param recordData
     */
    void logicEmbed(RecordData recordData);
}
