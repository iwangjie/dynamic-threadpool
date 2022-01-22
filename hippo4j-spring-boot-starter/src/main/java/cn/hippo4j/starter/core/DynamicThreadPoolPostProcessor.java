package cn.hippo4j.starter.core;

import cn.hippo4j.common.config.ApplicationContextHolder;
import cn.hippo4j.common.constant.Constants;
import cn.hippo4j.common.enums.EnableEnum;
import cn.hippo4j.common.model.PoolParameterInfo;
import cn.hippo4j.common.toolkit.JSONUtil;
import cn.hippo4j.common.web.base.Result;
import cn.hippo4j.starter.common.CommonDynamicThreadPool;
import cn.hippo4j.starter.config.BootstrapProperties;
import cn.hippo4j.starter.remote.HttpAgent;
import cn.hippo4j.starter.toolkit.DynamicThreadPoolAnnotationUtil;
import cn.hippo4j.starter.toolkit.thread.QueueTypeEnum;
import cn.hippo4j.starter.toolkit.thread.RejectedTypeEnum;
import cn.hippo4j.starter.toolkit.thread.ThreadPoolBuilder;
import cn.hippo4j.starter.wrapper.DynamicThreadPoolWrapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.task.TaskDecorator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static cn.hippo4j.common.constant.Constants.*;

/**
 * Dynamic threadPool post processor.
 *
 * @author chen.ma
 * @date 2021/8/2 20:40
 */
@Slf4j
@AllArgsConstructor
public final class DynamicThreadPoolPostProcessor implements BeanPostProcessor {

    private final BootstrapProperties properties;

    private final HttpAgent httpAgent;

    private final ThreadPoolOperation threadPoolOperation;

    private final ExecutorService executorService = ThreadPoolBuilder.builder()
            .corePoolSize(2)
            .maxPoolNum(4)
            .keepAliveTime(2000)
            .timeUnit(TimeUnit.MILLISECONDS)
            .workQueue(QueueTypeEnum.ARRAY_BLOCKING_QUEUE)
            .capacity(1024)
            .allowCoreThreadTimeOut(true)
            .threadFactory("client.dynamic.threadPool.change.config")
            .rejected(new ThreadPoolExecutor.AbortPolicy())
            .build();

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof DynamicThreadPoolExecutor) {
            DynamicThreadPool dynamicThreadPool;
            try {
                dynamicThreadPool = ApplicationContextHolder.findAnnotationOnBean(beanName, DynamicThreadPool.class);
                if (Objects.isNull(dynamicThreadPool)) {
                    // 适配低版本 SpringBoot
                    dynamicThreadPool = DynamicThreadPoolAnnotationUtil.findAnnotationOnBean(beanName, DynamicThreadPool.class);
                    if (Objects.isNull(dynamicThreadPool)) {
                        return bean;
                    }
                }
            } catch (Exception ex) {
                log.error("Failed to create dynamic thread pool in annotation mode.", ex);
                return bean;
            }

            DynamicThreadPoolExecutor dynamicExecutor = (DynamicThreadPoolExecutor) bean;
            // 包装本地线程池
            DynamicThreadPoolWrapper wrap = new DynamicThreadPoolWrapper(dynamicExecutor.getThreadPoolId(), dynamicExecutor);
            // 优先使用远程线程池配置否则使用本地线程池
            ThreadPoolExecutor remoteExecutor = fillPoolAndRegister(wrap);
            // 订阅远端更新
            subscribeConfig(wrap);

            return remoteExecutor;
        }

        // 如果直接就是一个包装器类型,那么直接注册
        if (bean instanceof DynamicThreadPoolWrapper) {
            DynamicThreadPoolWrapper wrap = (DynamicThreadPoolWrapper) bean;
            registerAndSubscribe(wrap);
        }

        return bean;
    }

    /**
     * Register and subscribe.
     *
     * @param dynamicThreadPoolWrap
     */
    protected void registerAndSubscribe(DynamicThreadPoolWrapper dynamicThreadPoolWrap) {
        fillPoolAndRegister(dynamicThreadPoolWrap);
        subscribeConfig(dynamicThreadPoolWrap);
    }

    /**
     * Fill the thread pool and register.
     *
     * @param dynamicThreadPoolWrap
     */
    protected ThreadPoolExecutor fillPoolAndRegister(DynamicThreadPoolWrapper dynamicThreadPoolWrap) {
        String tpId = dynamicThreadPoolWrap.getTpId();
        Map<String, String> queryStrMap = new HashMap<>(3);
        queryStrMap.put(TP_ID, tpId);
        queryStrMap.put(ITEM_ID, properties.getItemId());
        queryStrMap.put(NAMESPACE, properties.getNamespace());

        Result result;
        boolean isSubscribe = false;
        ThreadPoolExecutor newDynamicPoolExecutor = null;
        PoolParameterInfo ppi = new PoolParameterInfo();

        try {
            // 请求获取配置 get /configs
            result = httpAgent.httpGetByConfig(Constants.CONFIG_CONTROLLER_PATH, null, queryStrMap, 5000L);
            if (result.isSuccess() && result.getData() != null) {
                // 解析结果 操作两次,是否可优化?
                String resultJsonStr = JSONUtil.toJSONString(result.getData());
                if ((ppi = JSONUtil.parseObject(resultJsonStr, PoolParameterInfo.class)) != null) {
                    // 使用相关参数创建线程池
                    BlockingQueue workQueue = QueueTypeEnum.createBlockingQueue(ppi.getQueueType(), ppi.getCapacity());
                    // 使用远程参数重新创建线程池
                    newDynamicPoolExecutor = ThreadPoolBuilder.builder()
                            .dynamicPool()
                            .workQueue(workQueue)
                            .threadFactory(tpId)
                            .poolThreadSize(ppi.getCoreSize(), ppi.getMaxSize())
                            .keepAliveTime(ppi.getKeepAliveTime(), TimeUnit.SECONDS)
                            .rejected(RejectedTypeEnum.createPolicy(ppi.getRejectedType()))
                            .alarmConfig(ppi.getIsAlarm(), ppi.getCapacityAlarm(), ppi.getLivenessAlarm())
                            .allowCoreThreadTimeOut(EnableEnum.getBool(ppi.getAllowCoreThreadTimeOut()))
                            .build();

                    // 设置动态线程池增强参数
                    if (dynamicThreadPoolWrap.getExecutor() instanceof DynamicExecutorConfigurationSupport) {
                        TaskDecorator taskDecorator = ((DynamicThreadPoolExecutor) dynamicThreadPoolWrap.getExecutor()).getTaskDecorator();
                        ((DynamicThreadPoolExecutor) newDynamicPoolExecutor).setTaskDecorator(taskDecorator);

                        long awaitTerminationMillis = ((DynamicThreadPoolExecutor) dynamicThreadPoolWrap.getExecutor()).awaitTerminationMillis;
                        boolean waitForTasksToCompleteOnShutdown = ((DynamicThreadPoolExecutor) dynamicThreadPoolWrap.getExecutor()).waitForTasksToCompleteOnShutdown;
                        ((DynamicThreadPoolExecutor) newDynamicPoolExecutor).setSupportParam(awaitTerminationMillis, waitForTasksToCompleteOnShutdown);
                    }

                    // 替换掉了本地线程池
                    dynamicThreadPoolWrap.setExecutor(newDynamicPoolExecutor);
                    isSubscribe = true;
                }
            }
        } catch (Exception ex) {
            newDynamicPoolExecutor = dynamicThreadPoolWrap.getExecutor() != null ? dynamicThreadPoolWrap.getExecutor() : CommonDynamicThreadPool.getInstance(tpId);
            dynamicThreadPoolWrap.setExecutor(newDynamicPoolExecutor);

            log.error("Failed to initialize thread pool configuration. error message :: {}", ex.getMessage());
        } finally {
            if (Objects.isNull(dynamicThreadPoolWrap.getExecutor())) {
                dynamicThreadPoolWrap.setExecutor(CommonDynamicThreadPool.getInstance(tpId));
            }

            // 设置是否订阅远端线程池配置
            dynamicThreadPoolWrap.setSubscribeFlag(isSubscribe);
        }

        // 注册到应用全局
        GlobalThreadPoolManage.register(dynamicThreadPoolWrap.getTpId(), ppi, dynamicThreadPoolWrap);
        return newDynamicPoolExecutor;
    }

    /**
     * Client dynamic thread pool subscription server configuration.
     *
     * @param dynamicThreadPoolWrap
     */
    protected void subscribeConfig(DynamicThreadPoolWrapper dynamicThreadPoolWrap) {
        if (dynamicThreadPoolWrap.isSubscribeFlag()) {
            // 订阅并传递 callback
            threadPoolOperation.subscribeConfig(dynamicThreadPoolWrap.getTpId(), executorService, config -> ThreadPoolDynamicRefresh.refreshDynamicPool(config));
        }
    }

}
