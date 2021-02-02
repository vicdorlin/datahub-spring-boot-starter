package tech.jmeng.datahub.spring.boot.datahub;

import com.alibaba.fastjson.JSON;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.config.ProducerConfig;
import com.aliyun.datahub.clientlibrary.consumer.Consumer;
import com.aliyun.datahub.clientlibrary.producer.Producer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tech.jmeng.datahub.spring.boot.annotation.RuleCheck;
import tech.jmeng.datahub.spring.boot.datahub.vo.DataHubJobParamVo;
import tech.jmeng.datahub.spring.boot.datahub.vo.PageRequest;
import tech.jmeng.datahub.spring.boot.datahub.vo.PageResult;
import tech.jmeng.datahub.spring.boot.datahub.vo.request.*;
import tech.jmeng.datahub.spring.boot.prop.DataHubProperties;

import javax.annotation.PostConstruct;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.aliyun.datahub.client.model.ShardState.ACTIVE;

/**
 * datahub operator统一封装
 *
 * @author linx 2020-08-31 5:49 下午
 */
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Slf4j
@Service
public class DataHubOperator {
    private DatahubClient datahubClient;
    private final DataHubProperties dataHubProperties;

    @PostConstruct
    public void initDataHubClient() {
        // 创建DataHubClient实例
        this.datahubClient = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(new DatahubConfig(dataHubProperties.getEndpoint(),
                        // 是否开启二进制传输，服务端2.12版本开始支持
                        new AliyunAccount(dataHubProperties.getAccessId(), dataHubProperties.getAccessKey()), dataHubProperties.getEnableBinary()))
                // 专有云使用出错尝试将参数设置为false
                // HttpConfig可不设置，不设置时采用默认值
                // 读写数据推荐打开网络传输 LZ4压缩
                .setHttpConfig(dataHubProperties.getHttpConfig())
                .build();
    }

    /**
     * 协同消费：获取consumer
     *
     * @param consumerVo
     * @return
     */
    @RuleCheck(all = false, checkEmptyStringOrSet = true, fields = "project,topic,subId")
    private Consumer getConsumer(DataHubJobParamVo consumerVo) {
        ConsumerConfig config = new ConsumerConfig(
                dataHubProperties.getEndpoint(),
                dataHubProperties.getAccessId(),
                dataHubProperties.getAccessKey());

        return new Consumer(consumerVo.getProject(), consumerVo.getTopic(), consumerVo.getSubId(), config);
    }

    /**
     * 协同推送：获取producer
     *
     * @param bussVo
     * @return
     */
    @RuleCheck(all = false, checkEmptyStringOrSet = true, fields = "project,topic")
    private Producer getProducer(DataHubJobParamVo bussVo) {
        ProducerConfig config = new ProducerConfig(
                dataHubProperties.getEndpoint(),
                dataHubProperties.getAccessId(),
                dataHubProperties.getAccessKey());
        return new Producer(bussVo.getProject(), bussVo.getTopic(), config);
    }

    /**
     * 协同消费
     * datahub submit
     *
     * @param jobParamVo
     */
    public void read(DataHubJobParamVo jobParamVo) {
        log.info("dataHubJobSubmit start...");
        Consumer consumer = getConsumer(jobParamVo);
        int maxRetry = 3;
        boolean stop = false;
        try {
            while (!stop) {
                try {
                    while (true) {
                        // 协同消费刚初始化，需要等待服务端分配shard，约40秒，期间只能返回null
                        // 自动提交模式，每次调用read，认为之前读的数据都已处理完成，自动ack
                        RecordEntry record = consumer.read(maxRetry);
                        // 处理数据
                        if (record != null) {
                            try {
                                jobParamVo.getEmbedLogic().logicEmbed(record.getRecordData());
                            } catch (Exception e) {
                                log.error("project:{} topic:{} error occurred", jobParamVo.getProject(), jobParamVo.getTopic(), e.getMessage());
                                log.error("error:", e);
                            }
                            record.getKey().ack();
                        }
                    }
                } catch (SubscriptionOffsetResetException e) {
                    // 点位被重置，重新初始化consumer
                    try {
                        consumer.close();
                        consumer = getConsumer(jobParamVo);
                    } catch (DatahubClientException e1) {
                        // 初始化失败，重试或直接抛异常
                        log.error("create consumer failed DatahubClientException:", e);
                        throw new RuntimeException(e1);
                    }
                } catch (InvalidParameterException e) {
                    log.error("请求参数非法:", e);
                    break;
                } catch (AuthorizationFailureException e) {
                    log.error("签名不正确", e);
                    break;
                } catch (NoPermissionException e) {
                    log.error("没有权限", e);
                    break;
                } catch (SubscriptionOfflineException e) {
                    // 退出. Offline: 订阅下线;
                    log.warn("当前消费订阅已下线 消费停止 project:{} topic:{}", jobParamVo.getProject(), jobParamVo.getTopic());
                    break;
                } catch (SubscriptionSessionInvalidException e) {
                    //SessionChange: 表示订阅被其他客户端同时消费
                    //log.error("ERROR OCCURRED: 订阅被其他客户端同时消费", e);
                    log.warn("当前session已关闭 消费停止 project:{} topic:{} ", jobParamVo.getProject(), jobParamVo.getTopic());
                    break;
                } catch (DatahubClientException e) {
                    // 基类异常，包含网络问题等，可以选择重试
                    log.error("read failed, retry", e);
                    sleep(1000);
                } catch (RuntimeException e) {
                    log.error("consume RuntimeException:", e.getMessage());
                    break;
                } catch (Exception e) {
                    log.error("consume Exception:", e);
                    break;
                }
            }
        } catch (Throwable e) {
            log.error("read failed", e);
        } finally {
            // 确保资源正确释放
            // 会提交已ack的点位
            consumer.close();
        }
    }

    /**
     * 均匀写入
     *
     * @param requestVo
     */
    @RuleCheck(all = false, checkEmptyStringOrSet = true, fields = "projectName,topicName")
    public void send(Write2TupleTopicParamRequestVo requestVo) {
        RecordSchema recordSchema = this.getTopicSchema(TopicParamRequestVo.builder()
                .projectName(requestVo.getProjectName())
                .topicName(requestVo.getTopicName())
                .build());
        if (recordSchema == null) {
            throw new RuntimeException("recordSchema can not be null");
        }
        Producer producer = getProducer(DataHubJobParamVo.builder()
                .project(requestVo.getProjectName())
                .topic(requestVo.getTopicName()).build());
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (Write2TopicAttribute attribute : requestVo.getAttributes()) {
            TupleRecordData data = new TupleRecordData(recordSchema);
            Map<String, Object> fieldMap = attribute.getField();
            if (fieldMap == null || fieldMap.size() == 0) {
                throw new RuntimeException("数据异常");
            }
            for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
                Field field = recordSchema.getField(entry.getKey());
                try {
                    data.setField(entry.getKey(), entry.getValue());
                } catch (Exception e) {
                    //log.info("=======field name:{} type:{} nullable:{} entryKey:{} value:{}", field.getName(), field.getType().name(), field.isAllowNull(), entry.getKey(), entry.getValue());
                    /*if("lib".equals(entry.getKey())) {
                        data.setField(entry.getKey(),JSON.toJSONString(entry.getValue()));
                    }*/
                    data.setField(entry.getKey(), entry.getValue() == null ? null : JSON.toJSONString(entry.getValue()));
                }
            }
            RecordEntry recordEntry = new RecordEntry();
            Map<String, String> addition = attribute.getAddition();
            if (addition != null && addition.size() > 0) {
                for (Map.Entry<String, String> addEntry : addition.entrySet()) {
                    recordEntry.addAttribute(addEntry.getKey(), addEntry.getValue());
                }
            }
            recordEntry.setShardId(requestVo.getShardId());
            recordEntry.setRecordData(data);
            recordEntries.add(recordEntry);
        }
        int maxRetry = 3;
        while (true) {
            try {
                if (Strings.isBlank(requestVo.getShardId())) {
                    // 自动选择shard写入
                    producer.send(recordEntries, maxRetry);
                } else {
                    // 指定写入shard
                    producer.send(recordEntries, requestVo.getShardId(), maxRetry);
                }
                log.info("send records: {}", recordEntries.size());
                break;
            } catch (MalformedRecordException e) {
                // record 格式非法，根据业务场景选择忽略或直接抛异常
                log.error("write fail", e);
                throw e;
            } catch (InvalidParameterException e) {
                log.error("请求参数非法==", e);
                break;
            } catch (AuthorizationFailureException e) {
                log.error("签名不正确==", e);
                break;
            } catch (NoPermissionException e) {
                log.error("没有权限==", e);
                break;
            } catch (ShardNotFoundException e) {
                // shard 不存在, 如果不是写入自己指定的shard，可以不用处理
                log.error("shard不存在", e);
                sleep(1000);
            } catch (ResourceNotFoundException e) {
                // project, topic 或 shard 不存在
                log.error("project, topic 或 shard 不存在", e);
                throw e;
            } catch (DatahubClientException e) {
                // 基类异常，包含网络问题等，可以选择重试
                log.error("write fail", e);
                sleep(1000);
            }
        }
    }

    private void sleep(long milliSeconds) {
        try {
            TimeUnit.MILLISECONDS.sleep(milliSeconds);
        } catch (InterruptedException e) {
            log.error("fail to sleep with error:{}", e.getMessage());
        }
    }

    /**
     * 点位消费，并在消费过程中进行点位的提交
     * <p>
     * 关联订阅消费DataHub数据
     * 同读取DataHub数据类似，使用订阅进行消费的不同是订阅存储了消费的点位，用户可自由选择消费点位
     * 注意事项:
     * 首先调用openSubscriptionSession初始化offset，获取version + session信息，全局只初始化一次，多次调用此方法，会造成原有session失效，无法提交点位
     * 调用getcursor获取订阅的点位进行消费，消费完一条数据后，调用getNextCursor获取下一条数据点位，继续消费
     * 提交点位时，调用commitSubscriptionOffset提交点位，commit操作会检查version和session信息，必须完全一致才能提交成功
     */
    @RuleCheck(all = false, fields = "projectName,topicName,subId,shardId,sessionId,embedLogic", checkEmptyStringOrSet = true)
    public void consume(TopicParamRequestVo requestVo) {
        GetTopicResult getTopicResult = getTopic(requestVo);
        if (getTopicResult == null) {
            log.error("topic {}:{} not exist", requestVo.getProjectName(), requestVo.getTopicName());
            return;
        }
        /*InetAddress ia = InetAddress.getLocalHost();
        //获取计算机主机名
        String host = ia.getHostName();
        //获取计算机IP
        String ip = ia.getHostAddress();
        log.info("host:{} ip:{}", host, ip);*/
        /*Thread t = Thread.currentThread();
        String name = t.getName();
        log.info("consume started with thread:{}", name);*/

        String shardId = requestVo.getShardId();
        String cursor = getRealtimeCursor(requestVo);
        // 每次读取1条record
        int fetchNum = 1;

        RecordSchema schema = getTopicResult.getRecordSchema();
        while (true) {
            try {
                GetRecordsResult getRecordsResult = datahubClient.getRecords(requestVo.getProjectName(), requestVo.getTopicName(), shardId, schema, cursor, fetchNum);
                if (getRecordsResult.getRecordCount() <= 0) {
                    SubscriptionState subscriptionState = getSubscriptionState(requestVo);
                    if (SubscriptionState.OFFLINE.getValue() == subscriptionState.getValue()) {
                        log.warn("检测到订阅已下线 程序将自动停止...");
                        break;
                    }
                    cursor = getRealtimeCursor(requestVo);
                    log.info("project:{} topic:{} I'm alive ...", requestVo.getProjectName(), requestVo.getTopicName());
                    // 无数据，sleep后读取
                    Thread.sleep(6000);
                    continue;
                }
                for (RecordEntry recordEntry : getRecordsResult.getRecords()) {
                    //嵌入处理消费逻辑
                    try {
                        requestVo.getEmbedLogic().logicEmbed(recordEntry.getRecordData());
                    } catch (Exception e) {
                        log.error("error:", e);
                        log.error("project:{} topic:{} error occurred cursor:{}", requestVo.getProjectName(), requestVo.getTopicName(), e.getMessage(), cursor);
                    }
                    // 处理数据完成后，设置点位
                    Map<String, SubscriptionOffset> subscriptionOffsetMap = getSubscriptionOffset(requestVo);
                    SubscriptionOffset subscriptionOffset = subscriptionOffsetMap.get(shardId);
                    subscriptionOffset.setSessionId(requestVo.getSessionId());
                    subscriptionOffset.setSequence(recordEntry.getSequence());
                    subscriptionOffset.setTimestamp(recordEntry.getSystemTime());
                    //提交点位
                    Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
                    offsetMap.put(shardId, subscriptionOffset);
                    datahubClient.commitSubscriptionOffset(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getSubId(), offsetMap);
                    log.info("commit offset successful!! sequence:{} systemTime:{}", recordEntry.getSequence(), recordEntry.getSystemTime());
                }
                cursor = getRecordsResult.getNextCursor();
            } catch (SubscriptionOfflineException e) {
                // 退出. Offline: 订阅下线;
                log.warn("当前消费订阅已下线 消费停止 project:{} topic:{}", requestVo.getProjectName(), requestVo.getTopicName());
                break;
            } catch (SubscriptionSessionInvalidException e) {
                //SessionChange: 表示订阅被其他客户端同时消费
                //log.error("ERROR OCCURRED: 订阅被其他客户端同时消费", e);
                log.warn("当前session已关闭 消费停止 project:{} topic:{} ", requestVo.getProjectName(), requestVo.getTopicName());
                break;
            } catch (SubscriptionOffsetResetException e) {
                // 表示点位被重置，重新获取SubscriptionOffset信息，这里以Sequence重置为例
                // 如果以Timestamp重置，需要通过CursorType.SYSTEM_TIME获取cursor
                cursor = getRealtimeCursor(requestVo);
            } catch (DatahubClientException e) {
                log.error("consume DatahubClientException:", e);
            } catch (RuntimeException e) {
                log.error("consume RuntimeException:", e.getMessage());
                break;
            } catch (Exception e) {
                log.error("consume Exception:", e);
                break;
            }
        }
    }

    /**
     * 获取实时消费点位
     *
     * @param requestVo
     * @return
     */
    private String getRealtimeCursor(TopicParamRequestVo requestVo) {
        String shardId = requestVo.getShardId();
        Map<String, SubscriptionOffset> subscriptionOffsetMap = getSubscriptionOffset(requestVo);
        SubscriptionOffset subscriptionOffset = subscriptionOffsetMap.get(shardId);
        subscriptionOffset.setSessionId(requestVo.getSessionId());
        // 1、获取当前点位的cursor，如果当前点位已过期则获取生命周期内第一条record的cursor，未消费同样获取生命周期内第一条record的cursor
        String cursor = null;
        //sequence < 0说明未消费
        log.debug("subscriptionOffset.getSequence():{}", subscriptionOffset.getSequence());
        long sequence = subscriptionOffset.getSequence();
        if (sequence < 0) {
            // 获取生命周期内第一条record的cursor
            cursor = datahubClient.getCursor(requestVo.getProjectName(), requestVo.getTopicName(), shardId, CursorType.OLDEST).getCursor();
        } else {
            // 获取下一条记录的Cursor
            long nextSequence = subscriptionOffset.getSequence() + 1;
            try {
                //按照SEQUENCE getCursor可能报SeekOutOfRange错误，表示当前cursor的数据已过期
                cursor = datahubClient.getCursor(requestVo.getProjectName(), requestVo.getTopicName(), shardId, CursorType.SEQUENCE, nextSequence).getCursor();
            } catch (SeekOutOfRangeException e) {
                // 获取生命周期内第一条record的cursor
                log.debug("SeekOutOfRangeException occurred nextSequence:{}", nextSequence);
                cursor = datahubClient.getCursor(requestVo.getProjectName(), requestVo.getTopicName(), shardId, CursorType.LATEST).getCursor();
            }
        }
        return cursor;
    }

    /**
     * 创建订阅主题
     * 订阅服务提供了服务端保存用户消费点位的功能，只需要通过简单配置和处理，就可以实现高可用的点位存储服务。
     * comment 这样来写  {"application":"应用","description":"描述"}
     *
     * @param requestVo
     * @return
     */
    @RuleCheck(all = false, fields = "projectName,topicName,subscriptionComment", checkEmptyStringOrSet = true)
    public String createSubscription(TopicParamRequestVo requestVo) {
        CreateSubscriptionResult result = datahubClient.createSubscription(requestVo.getProjectName(), requestVo.getTopicName(), JSON.toJSONString(requestVo.getSubscriptionComment()));
        return result.getSubId();
    }

    @RuleCheck(all = false, fields = "projectName,topicName,subId", checkEmptyStringOrSet = true)
    public void deleteSubscription(TopicParamRequestVo requestVo) {
        datahubClient.deleteSubscription(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getSubId());
    }

    @RuleCheck(all = false, fields = "projectName,topicName,subId,subscriptionComment", checkEmptyStringOrSet = true)
    public void updateSubscription(TopicParamRequestVo requestVo) {
        datahubClient.updateSubscription(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getSubId(), JSON.toJSONString(requestVo.getSubscriptionComment()));
    }

    @RuleCheck(all = false, fields = "filter")
    public PageResult<SubscriptionEntry> listSubscription(PageRequest<TopicParamRequestVo> request) {
        Integer page = request.getPage();
        Integer size = request.getSize();
        if (page == null || page < 1) {
            page = 1;
        }
        if (size == null || size < 1) {
            size = 10;
        }
        TopicParamRequestVo filter = request.getFilter();
        String projectName = filter.getProjectName();
        String topicName = filter.getTopicName();
        if (Strings.isBlank(projectName) || Strings.isBlank(topicName)) {
            throw new RuntimeException("projectName和topicName不能为空");
        }
        ListSubscriptionResult result = datahubClient.listSubscription(filter.getProjectName(), filter.getTopicName(), page, size);
        return new PageResult<>(request, result.getTotalCount(), result.getSubscriptions());
    }

    @RuleCheck(all = false, fields = "projectName,topicName,subId", checkEmptyStringOrSet = true)
    public SubscriptionEntry getSubscription(TopicParamRequestVo requestVo) {
        return datahubClient.getSubscription(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getSubId());
    }

    @RuleCheck(all = false, fields = "projectName,topicName,subId", checkEmptyStringOrSet = true)
    public SubscriptionState getSubscriptionState(TopicParamRequestVo requestVo) {
        SubscriptionEntry subscription = getSubscription(requestVo);
        if (subscription == null) {
            throw new RuntimeException("找不到该订阅 topicParamRequestVo:" + requestVo);
        }
        return subscription.getState();
    }

    /**
     * 更新 Subscription 状态
     */
    @RuleCheck(all = false, fields = "projectName,topicName,subId,state", checkEmptyStringOrSet = true)
    public void updateSubscriptionState(TopicParamRequestVo requestVo) {
        datahubClient.updateSubscriptionState(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getSubId(), requestVo.getState());
    }

    /**
     * 一个subscription创建后，初始状态是未消费的，要使用subscription服务提供的点位存储功能，需要进行一些offset操作。
     * openSubscriptionSession只需要初始化一次，再次调用会重新生成一个消费sessionId，之前的session会失效，无法commit点位。
     *
     * @param requestVo
     */
    @RuleCheck(all = false, fields = "projectName,topicName,subId", checkEmptyStringOrSet = true)
    public Map<String, SubscriptionOffset> openSubscriptionSession(TopicParamRequestVo requestVo) {
        OpenSubscriptionSessionResult result = datahubClient.openSubscriptionSession(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getSubId(), getActiveShardIds(requestVo));
        return result.getOffsets();
    }

    @RuleCheck(all = false, fields = "projectName,topicName", checkEmptyStringOrSet = true)
    public List<String> getActiveShardIds(TopicParamRequestVo requestVo) {
        List<String> shardIds = new ArrayList<>();
        ListShardResult listShard = listShard(ShardParamRequestVo.builder().projectName(requestVo.getProjectName()).topicName(requestVo.getTopicName()).build());
        List<ShardEntry> shards;
        if (listShard == null || CollectionUtils.isEmpty((shards = listShard.getShards()))) {
            log.error("openSubscriptionSession failed for reason: no shards");
            return shardIds;
        }
        for (ShardEntry shard : shards) {
            if (shard.getState() == ACTIVE) {
                shardIds.add(shard.getShardId());
            }
        }
        return shardIds;
    }

    /**
     * 获取点位
     */
    @RuleCheck(all = false, fields = "projectName,topicName,subId", checkEmptyStringOrSet = true)
    public Map<String, SubscriptionOffset> getSubscriptionOffset(TopicParamRequestVo requestVo) {
        ListShardResult listShard = listShard(ShardParamRequestVo.builder().projectName(requestVo.getProjectName()).topicName(requestVo.getTopicName()).build());
        List<ShardEntry> shards;
        if (listShard == null || CollectionUtils.isEmpty((shards = listShard.getShards()))) {
            log.error("openSubscriptionSession failed for reason: no shards");
            return null;
        }
        List<String> shardIds = new ArrayList<>();
        for (ShardEntry shard : shards) {
            shardIds.add(shard.getShardId());
        }
        return datahubClient.getSubscriptionOffset(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getSubId(), shardIds).getOffsets();
    }

    /**
     * 提交点位 会验证versionId和sessionId，必须与当前的一致；提交的点位信息没有严格限制，建议按照record中的真实sequence和timestamp来填写。
     */
    @RuleCheck(all = false, fields = "projectName,topicName,subId,sequence,timestamp,shardId", checkEmptyStringOrSet = true)
    public void commitSubscriptionOffset(TopicParamRequestVo requestVo) {
        /*List<String> activeShardIds = getActiveShardIds(requestVo);
        String shardIdChosen = null;
        if (activeShardIds != null && activeShardIds.size() > 0) {
            shardIdChosen = activeShardIds.get(new Random().nextInt(activeShardIds.size()));
        }*/
        Map<String, SubscriptionOffset> offsetMap = getSubscriptionOffset(requestVo);
        SubscriptionOffset subscriptionOffset = offsetMap.get(requestVo.getShardId());
        subscriptionOffset.setSequence(requestVo.getSequence());
        subscriptionOffset.setTimestamp(requestVo.getTimestamp());
        Map<String, SubscriptionOffset> offsets = new HashMap<String, SubscriptionOffset>();
        offsets.put(requestVo.getShardId(), subscriptionOffset);
        // 提交点位
        datahubClient.commitSubscriptionOffset(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getSubId(), offsets);
    }

    /**
     * 重置点位 可以将消费点位设置到某个时间点，如果在这个时间点有多个record，那么点位会设置到该时间点的第一条record的位置。
     * 重置点位在修改点位信息的同时更新versionId，运行中的任务在使用旧的versionId来提交点位时会收到SubscriptionOffsetResetException，
     * 通过getSubscriptionOffset接口可以拿到新的versionId。
     */
    @RuleCheck(all = false, fields = "projectName,topicName,subId", checkEmptyStringOrSet = true)
    public void resetSubscriptionOffset(TopicParamRequestVo requestVo) {
        //选择想要重置点位到的时间，并转换为时间戳
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long timestamp = requestVo.getTimestamp();
        if (timestamp == null) {
            if (requestVo.getTime() == null) {
                throw new RuntimeException("timestamp或者time必须至少指定一个");
            }
            try {
                Date date = simpleDateFormat.parse(requestVo.getTime());
                timestamp = date.getTime();//获取时间的时间戳
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
        SubscriptionOffset offset = new SubscriptionOffset();
        offset.setTimestamp(timestamp);
        Map<String, SubscriptionOffset> offsets = new HashMap<>();
        List<String> activeShardIds = getActiveShardIds(requestVo);
        for (String shardId : activeShardIds) {
            offsets.put(shardId, offset);
        }
        datahubClient.resetSubscriptionOffset(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getSubId(), offsets);
    }

    @RuleCheck
    public void createProject(ProjectParamRequestVo requestVo) {
        datahubClient.createProject(requestVo.getProjectName(), requestVo.getComment());
        log.info("create project:{} successful", requestVo.getProjectName());
    }

    public void deleteProject(ProjectParamRequestVo requestVo) {
        datahubClient.deleteProject(requestVo.getProjectName());
        log.info("delete project:{} successful", requestVo.getProjectName());
    }

    @RuleCheck
    public void updateProject(ProjectParamRequestVo requestVo) {
        datahubClient.updateProject(requestVo.getProjectName(), requestVo.getComment());
        log.info("update project:{} successful", requestVo.getProjectName());
    }

    @RuleCheck(all = false, fields = "projectName", checkEmptyStringOrSet = true)
    public GetProjectResult getProject(ProjectParamRequestVo requestVo) {
        GetProjectResult getProjectResult = datahubClient.getProject(requestVo.getProjectName());
        return getProjectResult;
    }

    public ListProjectResult listProject() {
        ListProjectResult listProjectResult = datahubClient.listProject();
        return listProjectResult;
    }

    @RuleCheck(all = false, fields = "projectName,topicName,topicComment,fields", checkEmptyStringOrSet = true)
    public void createTupleTopic(TopicParamRequestVo requestVo) {
        RecordSchema schema = new RecordSchema();
        for (PlainField field : requestVo.getFields()) {
            FieldType fieldType = FieldType.valueOf(field.getType());
            schema.addField(new Field(field.getName(), fieldType, field.isAllowNull()));
        }
        datahubClient.createTopic(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getShardCount(), requestVo.getLifeCycle(), RecordType.TUPLE, schema, requestVo.getTopicComment());
        log.info("create project_topic:{}_{} successful", requestVo.getProjectName(), requestVo.getTopicName());
    }

    /**
     * 原模原样 拷贝一个topic
     *
     * @param originProjectName
     * @param originTopicName
     * @param newProjectName
     * @param newTopicName
     * @param comment
     */
    @RuleCheck
    public void copyTupleTopic(String originProjectName, String originTopicName, String newProjectName, String newTopicName, String comment) {
        GetTopicResult originTopic = getTopic(TopicParamRequestVo.builder().projectName(originProjectName).topicName(originTopicName).build());
        List<Field> fields = originTopic.getRecordSchema().getFields();
        if (comment == null) {
            comment = originTopic.getComment();
        }
        createTupleTopic(TopicParamRequestVo.builder()
                .projectName(newProjectName)
                .topicName(newTopicName)
                .shardCount(originTopic.getShardCount())
                .lifeCycle(originTopic.getLifeCycle())
                .topicComment(comment).fields(
                fields.stream().map(x -> new PlainField(x.getName(), x.getType().name(), x.isAllowNull())).collect(Collectors.toList())
        ).build());
    }

    @RuleCheck(all = false, fields = "projectName,topicName", checkEmptyStringOrSet = true)
    public void deleteTopic(TopicParamRequestVo requestVo) {
        datahubClient.deleteTopic(requestVo.getProjectName(), requestVo.getTopicName());
        log.info("delete project_topic:{}_{} successful", requestVo.getProjectName(), requestVo.getTopicName());
    }

    @RuleCheck(all = false, fields = "projectName", checkEmptyStringOrSet = true)
    public ListTopicResult listTopic(ProjectParamRequestVo requestVo) {
        ListTopicResult listTopicResult = datahubClient.listTopic(requestVo.getProjectName());
        return listTopicResult;
    }

    /**
     * 只更新lifeCycle和topicComment
     *
     * @param requestVo
     */
    @RuleCheck(all = false, fields = "projectName,topicName,topicComment", checkEmptyStringOrSet = true)
    public void updateTopic(TopicParamRequestVo requestVo) {
        datahubClient.updateTopic(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getLifeCycle(), requestVo.getTopicComment());
        log.info("update project_topic:{}_{} successful", requestVo.getProjectName(), requestVo.getTopicName());
    }

    @RuleCheck(all = false, fields = "projectName,topicName", checkEmptyStringOrSet = true)
    public GetTopicResult getTopic(TopicParamRequestVo requestVo) {
        try {
            GetTopicResult getTopicResult = datahubClient.getTopic(requestVo.getProjectName(), requestVo.getTopicName());
            return getTopicResult;
        } catch (ResourceNotFoundException e) {
            log.warn("project:{} topic:{} not exist", requestVo.getProjectName(), requestVo.getTopicName());
            return null;
        }
    }

    @RuleCheck(all = false, fields = "projectName,topicName,fields", checkEmptyStringOrSet = true)
    public void appendField(TopicParamRequestVo requestVo) {
        for (PlainField plainField : requestVo.getFields()) {
            Field newField = new Field(plainField.getName(), FieldType.valueOf(plainField.getType()), plainField.isAllowNull());
            datahubClient.appendField(requestVo.getProjectName(), requestVo.getTopicName(), newField);
            log.info("append field:{} successful", plainField);
        }
    }

    @RuleCheck(all = false, fields = "projectName,topicName", checkEmptyStringOrSet = true)
    public ListShardResult listShard(ShardParamRequestVo requestVo) {
        ListShardResult listShardResult = datahubClient.listShard(requestVo.getProjectName(), requestVo.getTopicName());
        return listShardResult;
    }

    @RuleCheck
    public void splitShard(ShardParamRequestVo requestVo) {
        datahubClient.splitShard(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getShardId());
        log.info("split shard projectName:{} topicName:{} shardId:{} success", requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getShardId());
    }

    /**
     * 合并一个Topic中两个处于ACTIVE状态的Shard,要求两个Shard的位置必须相邻。
     * 每个Shard相邻的两个Shard可以参考listShard的结果。
     * 这里将会合并shardId对应shard与其后一个active的shard
     *
     * @param requestVo
     */
    @RuleCheck
    public void mergeShard(ShardParamRequestVo requestVo) {
        ListShardResult listShardResult = listShard(requestVo);
        List<ShardEntry> shards = listShardResult.getShards();
        if (listShardResult == null || shards == null || shards.size() < 2) {
            throw new RuntimeException("没有合适的合并目标");
        }
        for (ShardEntry shard : shards) {
            if (!requestVo.getShardId().equals(shard.getShardId())) {
                continue;
            }
            if (!ACTIVE.equals(shard.getState())) {
                throw new RuntimeException("不能合并非active的shard");
            }
            if (shard.getRightShardId() == null) {
                throw new RuntimeException("该shard无向后可合并的目标");
            }
            datahubClient.mergeShard(requestVo.getProjectName(), requestVo.getTopicName(), requestVo.getShardId(), shard.getRightShardId());
            log.info("merge shard projectName:{} topicName:{} shardId: {} and {} success!", requestVo.getProjectName(), requestVo.getTopicName(), shard.getShardId(), shard.getRightShardId());
            break;
        }
    }

    @RuleCheck(all = false, checkEmptyStringOrSet = true, fields = "projectName,topicName")
    public RecordSchema getTopicSchema(TopicParamRequestVo requestVo) {
        return datahubClient.getTopic(requestVo.getProjectName(), requestVo.getTopicName()).getRecordSchema();
    }

    /**
     * 写入Tuple型数据
     */
    @RuleCheck
    public void write2TupleTopic(Write2TupleTopicParamRequestVo requestVo) {
        // 获取schema
        RecordSchema recordSchema = getTopicSchema(TopicParamRequestVo.builder()
                .projectName(requestVo.getProjectName())
                .topicName(requestVo.getTopicName())
                .build());
        List<RecordEntry> records = new ArrayList<>();
        for (Write2TopicAttribute attribute : requestVo.getAttributes()) {
            TupleRecordData data = new TupleRecordData(recordSchema);
            Map<String, Object> fieldMap = attribute.getField();
            if (fieldMap == null || fieldMap.size() == 0) {
                throw new RuntimeException("数据异常");
            }
            for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
                data.setField(entry.getKey(), entry.getValue());
            }
            RecordEntry recordEntry = new RecordEntry();
            Map<String, String> addition = attribute.getAddition();
            if (addition != null && addition.size() > 0) {
                for (Map.Entry<String, String> addEntry : addition.entrySet()) {
                    recordEntry.addAttribute(addEntry.getKey(), addEntry.getValue());
                }
            }
            recordEntry.setShardId(requestVo.getShardId());
            recordEntry.setRecordData(data);
            records.add(recordEntry);
        }
        int i = datahubClient.putRecords(requestVo.getProjectName(), requestVo.getTopicName(), records).getFailedRecordCount();
        log.info("put record project:{} topic:{} records:{} failedCount:{}", requestVo.getProjectName(), requestVo.getTopicName(), records, i);
        if (i > 0) {
            retry(records, 1, requestVo.getProjectName(), requestVo.getTopicName());
        }
    }

    //重试机制
    public void retry(List<RecordEntry> records, int retryTimes, String project, String topic) {
        boolean suc = false;
        while (retryTimes != 0) {
            retryTimes = retryTimes - 1;
            int failedNum = datahubClient.putRecords(project, topic, records).getFailedRecordCount();
            if (failedNum > 0) {
                continue;
            }
            suc = true;
            break;
        }
        if (!suc) {
            log.info("retryFailure");
        }
    }
}
