package cn.hippo4j.starter.core;

import cn.hippo4j.common.model.PoolParameterInfo;
import cn.hippo4j.common.toolkit.ContentUtil;
import cn.hippo4j.common.toolkit.GroupKey;
import cn.hippo4j.common.toolkit.JSONUtil;
import cn.hippo4j.common.web.base.Result;
import cn.hippo4j.starter.remote.HttpAgent;
import cn.hippo4j.starter.remote.ServerHealthCheck;
import cn.hippo4j.starter.toolkit.thread.ThreadFactoryBuilder;
import cn.hutool.core.util.IdUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.*;

import static cn.hippo4j.common.constant.Constants.*;

/**
 * Client worker.
 *
 * @author chen.ma
 * @date 2021/6/20 18:34
 */
@Slf4j
public class ClientWorker {

    private long timeout;

    // 渴望计数?
    private double currentLongingTaskCount = 0;

    private final HttpAgent agent;

    private final String identification;

    private final ServerHealthCheck serverHealthCheck;

    private final ScheduledExecutorService executor;

    private final ScheduledExecutorService executorService;

    private final CountDownLatch awaitApplicationComplete = new CountDownLatch(1);

    private final ConcurrentHashMap<String, CacheData> cacheMap = new ConcurrentHashMap(16);

    @SuppressWarnings("all")
    public ClientWorker(HttpAgent httpAgent, String identification, ServerHealthCheck serverHealthCheck) {
        this.agent = httpAgent;
        this.identification = identification;
        this.timeout = CONFIG_LONG_POLL_TIMEOUT;
        this.serverHealthCheck = serverHealthCheck;

        this.executor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r);
            t.setName("client.worker.executor");
            t.setDaemon(true);
            return t;
        });

        this.executorService = Executors.newSingleThreadScheduledExecutor(
                ThreadFactoryBuilder.builder().prefix("client.long.polling.executor").daemon(true).build()
        );

        log.info("Client identity :: {}", identification);

        this.executor.scheduleWithFixedDelay(() -> {
            try {
                // 等待 spring 容器启动成功
                awaitApplicationComplete.await();

                // 检查动态线程池配置是否被更新
                checkConfigInfo();
            } catch (Throwable e) {
                log.error("Sub check rotate check error.", e);
            }
        }, 1L, 1024L, TimeUnit.MILLISECONDS);
    }

    // 循环注册长连接线程池配置更新检查
    public void checkConfigInfo() {
        int listenerSize = cacheMap.size();
        double perTaskConfigSize = 3000D;
        // 计算需要挂多少个线程来轮训配置,3000个线程池用一个线程来轮询
        int longingTaskCount = (int) Math.ceil(listenerSize / perTaskConfigSize);

        // 如果这次检查的时候比原来多了,追加新线程
        if (longingTaskCount > currentLongingTaskCount) {
            for (int i = (int) currentLongingTaskCount; i < longingTaskCount; i++) {
                // 注册
                executorService.execute(new LongPollingRunnable());
            }
            currentLongingTaskCount = longingTaskCount;
        }
    }

    /**
     * 长连接轮训配置实现
     */
    class LongPollingRunnable implements Runnable {

        @Override
        @SneakyThrows
        public void run() {
            // 判断服务器健康
            serverHealthCheck.isHealthStatus();

            List<CacheData> cacheDataList = new ArrayList();
            List<String> inInitializingCacheList = new ArrayList();
            cacheMap.forEach((key, val) -> cacheDataList.add(val));

            // 获取变更了配置的线程池id列表
            List<String> changedTpIds = checkUpdateDataIds(cacheDataList, inInitializingCacheList);
            for (String groupKey : changedTpIds) {
                // + 号分隔  tpId+itemId+namespace
                String[] keys = groupKey.split(GROUP_KEY_DELIMITER_TRANSLATION);
                String tpId = keys[0];
                String itemId = keys[1];
                String namespace = keys[2];

                try {
                    // 获取详细的变更
                    String content = getServerConfig(namespace, itemId, tpId, 3000L);
                    CacheData cacheData = cacheMap.get(tpId);
                    String poolContent = ContentUtil.getPoolContent(JSONUtil.parseObject(content, PoolParameterInfo.class));
                    // 更新缓存
                    cacheData.setContent(poolContent);
                } catch (Exception ex) {
                    // ignore
                    log.error("Failed to get the latest thread pool configuration.", ex);
                }
            }

            // 循环缓存,目的是将 md5 不一致的通知变更,很骚很骚!
            for (CacheData cacheData : cacheDataList) {
                if (!cacheData.isInitializing() || inInitializingCacheList
                        .contains(GroupKey.getKeyTenant(cacheData.tpId, cacheData.itemId, cacheData.tenantId))) {
                    cacheData.checkListenerMd5();
                    cacheData.setInitializing(false);
                }
            }

            inInitializingCacheList.clear();
            // 把自己重新放回线程池.... 好骚的操作
            executorService.execute(this);
        }
    }

    private List<String> checkUpdateDataIds(List<CacheData> cacheDataList, List<String> inInitializingCacheList) {
        StringBuilder sb = new StringBuilder();
        for (CacheData cacheData : cacheDataList) {
            sb.append(cacheData.tpId).append(WORD_SEPARATOR);
            sb.append(cacheData.itemId).append(WORD_SEPARATOR);
            sb.append(cacheData.tenantId).append(WORD_SEPARATOR);
            sb.append(identification).append(WORD_SEPARATOR);
            sb.append(cacheData.getMd5()).append(LINE_SEPARATOR);

            if (cacheData.isInitializing()) {
                inInitializingCacheList.add(GroupKey.getKeyTenant(cacheData.tpId, cacheData.itemId, cacheData.tenantId));
            }
        }

        boolean isInitializingCacheList = !inInitializingCacheList.isEmpty();
        return checkUpdateTpIds(sb.toString(), isInitializingCacheList);
    }

    public List<String> checkUpdateTpIds(String probeUpdateString, boolean isInitializingCacheList) {
        Map<String, String> params = new HashMap(2);
        params.put(PROBE_MODIFY_REQUEST, probeUpdateString);
        params.put(WEIGHT_CONFIGS, IdUtil.simpleUUID());
        Map<String, String> headers = new HashMap(2);
        // 长连接
        headers.put(LONG_PULLING_TIMEOUT, "" + timeout);

        // 确认客户端身份, 修改线程池配置时可单独修改
        headers.put(LONG_PULLING_CLIENT_IDENTIFICATION, identification);

        // told server do not hang me up if new initializing cacheData added in
        if (isInitializingCacheList) {
            headers.put(LONG_PULLING_TIMEOUT_NO_HANGUP, "true");
        }

        if (StringUtils.isEmpty(probeUpdateString)) {
            return Collections.emptyList();
        }

        try {
            long readTimeoutMs = timeout + (long) Math.round(timeout >> 1);
            // 发送监听请求 长连接 等待 3000ms
            Result result = agent.httpPostByConfig(LISTENER_PATH, headers, params, readTimeoutMs);

            if (result != null && result.isSuccess()) {
                // 处理响应
                return parseUpdateDataIdResponse(result.getData().toString());
            }
        } catch (Exception ex) {
            setHealthServer(false);
            log.error("Check update get changed dataId exception. error message :: {}", ex.getMessage());
        }

        return Collections.emptyList();
    }

    public String getServerConfig(String namespace, String itemId, String tpId, long readTimeout) {
        Map<String, String> params = new HashMap(3);
        params.put("namespace", namespace);
        params.put("itemId", itemId);
        params.put("tpId", tpId);
        params.put("instanceId", identification);

        Result result = agent.httpGetByConfig(CONFIG_CONTROLLER_PATH, null, params, readTimeout);
        if (result.isSuccess()) {
            return JSONUtil.toJSONString(result.getData());
        }

        log.error("Sub server namespace :: {}, itemId :: {}, tpId :: {}, result code :: {}",
                namespace, itemId, tpId, result.getCode());
        return NULL;
    }

    public List<String> parseUpdateDataIdResponse(String response) {
        if (StringUtils.isEmpty(response)) {
            return Collections.emptyList();
        }

        try {
            response = URLDecoder.decode(response, "UTF-8");
        } catch (Exception e) {
            log.error("Polling resp decode modifiedDataIdsString error.", e);
        }


        List<String> updateList = new LinkedList();
        for (String dataIdAndGroup : response.split(LINE_SEPARATOR)) {
            if (!StringUtils.isEmpty(dataIdAndGroup)) {
                String[] keyArr = dataIdAndGroup.split(WORD_SEPARATOR);
                String dataId = keyArr[0];
                String group = keyArr[1];
                if (keyArr.length == 3) {
                    String tenant = keyArr[2];
                    updateList.add(GroupKey.getKeyTenant(dataId, group, tenant));
                    log.info("Refresh thread pool changed. [{}]", dataId);
                } else {
                    log.error("[{}] Polling resp invalid dataIdAndGroup error.", dataIdAndGroup);
                }
            }
        }

        return updateList;
    }

    public void addTenantListeners(String namespace, String itemId, String tpId, List<? extends Listener> listeners) {
        CacheData cacheData = addCacheDataIfAbsent(namespace, itemId, tpId);
        for (Listener listener : listeners) {
            cacheData.addListener(listener);
        }
    }

    public CacheData addCacheDataIfAbsent(String namespace, String itemId, String tpId) {
        CacheData cacheData = cacheMap.get(tpId);
        if (cacheData != null) {
            return cacheData;
        }

        cacheData = new CacheData(namespace, itemId, tpId);
        CacheData lastCacheData = cacheMap.putIfAbsent(tpId, cacheData);
        if (lastCacheData == null) {
            String serverConfig;
            try {
                serverConfig = getServerConfig(namespace, itemId, tpId, 3000L);
                PoolParameterInfo poolInfo = JSONUtil.parseObject(serverConfig, PoolParameterInfo.class);
                cacheData.setContent(ContentUtil.getPoolContent(poolInfo));
            } catch (Exception ex) {
                log.error("Cache Data Error. Service Unavailable :: {}", ex.getMessage());
            }

            int taskId = cacheMap.size() / CONFIG_LONG_POLL_TIMEOUT;
            cacheData.setTaskId(taskId);

            lastCacheData = cacheData;
        }

        return lastCacheData;
    }

    private void setHealthServer(boolean isHealthServer) {
        this.serverHealthCheck.setHealthStatus(isHealthServer);
    }

    protected void notifyApplicationComplete() {
        awaitApplicationComplete.countDown();
    }

}
