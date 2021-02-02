package tech.jmeng.datahub.spring.boot.prop;

/**
 * @author linx 2020-11-30 下午4:10
 */

import com.aliyun.datahub.client.http.HttpConfig;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@Setter
@Getter
@ConfigurationProperties(prefix = "spring.datahub")
public class DataHubProperties {
    private String endpoint;
    private String accessId;
    private String accessKey;
    private Boolean enableBinary = false;
    @NestedConfigurationProperty
    private HttpConfig httpConfig;
}
