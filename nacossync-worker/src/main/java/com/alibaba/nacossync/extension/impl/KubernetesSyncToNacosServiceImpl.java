package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.KubernetesServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointPort;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

/**
 * kubernetes service sync to nacos
 *
 * @author shenyong
 * @date 2019/12/14
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.KUBERNETES, destinationCluster = ClusterTypeEnum.NACOS)
public class KubernetesSyncToNacosServiceImpl implements SyncService {

    private final MetricsManager metricsManager;
    private final KubernetesServerHolder kubernetesServerHolder;
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    private final NacosServerHolder nacosServerHolder;
    private final SpecialSyncEventBus specialSyncEventBus;

    public KubernetesSyncToNacosServiceImpl(MetricsManager metricsManager, KubernetesServerHolder kubernetesServerHolder,
                                            SkyWalkerCacheServices skyWalkerCacheServices, NacosServerHolder nacosServerHolder,
                                            SpecialSyncEventBus specialSyncEventBus) {
        this.metricsManager = metricsManager;
        this.kubernetesServerHolder = kubernetesServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.specialSyncEventBus = specialSyncEventBus;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        try {
            specialSyncEventBus.unsubscribe(taskDO);
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), taskDO.getNameSpace());
            List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName());
            for (Instance instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO)) {
                    destNamingService.deregisterInstance(taskDO.getServiceName(), instance.getIp(), instance.getPort());
                }
            }
        } catch (Exception e) {
            log.error("delete task from kubernetes to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            KubernetesClient client = kubernetesServerHolder.get(taskDO.getSourceClusterId(), taskDO.getNameSpace());
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), taskDO.getNameSpace());
            List<Endpoints> endpointsList = StringUtils.isEmpty(taskDO.getNameSpace()) ?
                    client.endpoints().inAnyNamespace().withField("metadata.name", taskDO.getServiceName()).list().getItems() :
                    Collections.singletonList(client.endpoints().withName(taskDO.getServiceName()).get());

            List<EndpointSubsetNs> subsetsNs = endpointsList.stream()
                    .map(endpoints -> getSubsetsFromEndpoints(client, endpoints))
                    .collect(Collectors.toList());

            if (!subsetsNs.isEmpty()) {
                Set<String> instanceKeySet = new HashSet<>();
                for (EndpointSubsetNs es : subsetsNs) {
                    if (needSync(es.getMetadata())) {
                        for (Instance instance : getNamespaceServiceInstances(client, es, taskDO)) {
                            destNamingService.registerInstance(taskDO.getServiceName(), instance);
                            instanceKeySet.add(composeInstanceKey(instance.getIp(), instance.getPort()));
                        }
                    }
                }
                List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName());
                for (Instance instance : allInstances) {
                    if (needDelete(instance.getMetadata(), taskDO)) {
                        destNamingService.deregisterInstance(taskDO.getServiceName(), instance.getIp(), instance.getPort());
                    }
                }
                specialSyncEventBus.subscribe(taskDO, this::sync);
            }
        } catch (Exception e) {
            log.error("sync task from consul to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    private List<Instance> getNamespaceServiceInstances(KubernetesClient client, EndpointSubsetNs es, TaskDO taskDO) {
        List<Instance> instances = new ArrayList<>();
        String namespace = es.getNamespace();
        List<EndpointSubset> subsets = es.getEndpointSubset();
        if (!subsets.isEmpty()) {
            final Service service = client.services().inNamespace(namespace)
                    .withName(taskDO.getServiceName()).get();
            final Map<String, String> serviceMetadata = this.getServiceMetadata(service);

            for (EndpointSubset s : subsets) {
                // Extend the service metadata map with per-endpoint port information (if
                // requested)
                Map<String, String> endpointMetadata = new HashMap<>(serviceMetadata);
                Map<String, String> portMetadata = s.getPorts().stream()
                        .filter(port -> !StringUtils.isEmpty(port.getName()))
                        .collect(toMap(EndpointPort::getName, port -> Integer.toString(port.getPort())));

                if (log.isDebugEnabled()) {
                    log.debug("Adding port metadata: " + portMetadata);
                }
                endpointMetadata.putAll(portMetadata);

                List<EndpointAddress> addresses = s.getAddresses();
                for (EndpointAddress endpointAddress : addresses) {
                    EndpointPort endpointPort = findEndpointPort(s);
                    Instance temp = new Instance();
                    temp.setIp(endpointAddress.getIp());
                    temp.setPort(endpointPort.getPort());
                    Map<String, String> metaData = new HashMap<>(endpointMetadata);
                    metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
                    metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
                            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
                    metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
                    temp.setMetadata(metaData);
                    instances.add(temp);
                }
            }
        }
        return instances;
    }

    private EndpointPort findEndpointPort(EndpointSubset s) {
        List<EndpointPort> ports = s.getPorts();
        EndpointPort endpointPort;
        if (ports.size() == 1) {
            endpointPort = ports.get(0);
        } else {
            Predicate<EndpointPort> portPredicate = port -> true;
            endpointPort = ports.stream().filter(portPredicate).findAny()
                    .orElseThrow(IllegalStateException::new);
        }
        return endpointPort;
    }

    private Map<String, String> getServiceMetadata(Service service) {
        Map<String, String> labelMetadata = service.getMetadata().getLabels();
        if (log.isDebugEnabled()) {
            log.debug("Adding label metadata: " + labelMetadata);
        }
        final Map<String, String> serviceMetadata = new HashMap<>(labelMetadata);
        Map<String, String> annotationMetadata = service.getMetadata().getAnnotations();
        if (log.isDebugEnabled()) {
            log.debug("Adding annotation metadata: " + annotationMetadata);
        }
        serviceMetadata.putAll(annotationMetadata);
        return serviceMetadata;
    }

    private EndpointSubsetNs getSubsetsFromEndpoints(KubernetesClient client, Endpoints endpoints) {
        EndpointSubsetNs es = new EndpointSubsetNs();
        // start with the default that comes
        es.setNamespace(client.getNamespace());
        // with the client

        if (endpoints != null && endpoints.getSubsets() != null) {
            es.setNamespace(endpoints.getMetadata().getNamespace());
            es.setEndpointSubset(endpoints.getSubsets());
        }

        return es;
    }

    private String composeInstanceKey(String ip, int port) {
        return ip + ":" + port;
    }

    private static class EndpointSubsetNs {
        private String namespace;

        private List<EndpointSubset> endpointSubset;

        private Map<String, String> metadata;

        public EndpointSubsetNs() {
            endpointSubset = new ArrayList<>();
        }

        public String getNamespace() {
            return namespace;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }

        public List<EndpointSubset> getEndpointSubset() {
            return endpointSubset;
        }

        public void setEndpointSubset(List<EndpointSubset> endpointSubset) {
            this.endpointSubset = endpointSubset;
        }

        public Map<String, String> getMetadata() {
            return metadata;
        }

        public void setMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
        }

        @Override
        public boolean equals(Object o) {
            return this.endpointSubset.equals(o);
        }

        @Override
        public int hashCode() {
            return this.endpointSubset.hashCode();
        }
    }
}
