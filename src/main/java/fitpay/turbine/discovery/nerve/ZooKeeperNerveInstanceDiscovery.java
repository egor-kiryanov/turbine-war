package fitpay.turbine.discovery.nerve;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParamBean;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.discovery.InstanceDiscovery;

import fitpay.turbine.model.Health;
import fitpay.turbine.model.Service;

public class ZooKeeperNerveInstanceDiscovery implements InstanceDiscovery, Watcher {
    private static Logger log = LoggerFactory
            .getLogger(ZooKeeperNerveInstanceDiscovery.class);

    private final static DynamicStringProperty zkHosts = DynamicPropertyFactory
            .getInstance().getStringProperty("turbine.ZkDiscovery.zkHosts",
                    null);
    private final static DynamicIntProperty timeout = DynamicPropertyFactory
            .getInstance().getIntProperty("turbine.ZkDiscovery.timeout", 3000);

    private final static DynamicStringProperty rootServicePath = DynamicPropertyFactory
            .getInstance().getStringProperty(
                    "turbine.ZkDiscovery.rootServicePath", "/nerve/services");
    private final static DynamicStringProperty servicesSuffixPath = DynamicPropertyFactory
            .getInstance().getStringProperty(
                    "turbine.ZkDiscovery.servicesSuffixPath", "services");

    private final static DynamicStringProperty clusterName = DynamicPropertyFactory
            .getInstance().getStringProperty("turbine.ZkDiscovery.clusterName",
                    "Default");

    private final static DynamicStringProperty healthPath = DynamicPropertyFactory
            .getInstance().getStringProperty("health.path", "/health");

    private final static DynamicStringProperty protocol = DynamicPropertyFactory
            .getInstance().getStringProperty("health.protocol", "http");
    
    private final static DynamicIntProperty connectionTimeout = DynamicPropertyFactory
            .getInstance().getIntProperty("heath.connectionTimeout", 500);
    
    private final static DynamicIntProperty readTimeout = DynamicPropertyFactory
            .getInstance().getIntProperty("heath.readTimeout", 1000);
    
    private final static ObjectMapper om = new ObjectMapper();

    private final HttpClient httpClient;
    
    public ZooKeeperNerveInstanceDiscovery() {
        HttpParams params = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(params, connectionTimeout.get());
        HttpConnectionParams.setSoTimeout(params, readTimeout.get());
        
        this.httpClient = new DefaultHttpClient(params);
    }
    
    @Override
    public Collection<Instance> getInstanceList() throws Exception {
        Collection<Instance> instances = new ArrayList<Instance>();

        log.debug("connecting to zookeeper hosts: {}", zkHosts.getValue());
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(zkHosts.getValue(), timeout.getValue(), this);

            for (String serviceName : zk.getChildren(
                    rootServicePath.getValue(), false)) {
                log.debug("discovering instances for service: {}", serviceName);
                instances.addAll(getServiceInstances(zk, serviceName));
            }
        } finally {
            if (zk != null) {
                log.debug("closing the zk connection");
                zk.close();
            }
        }

        log.debug("discovered instances: {}", instances);
        return instances;
    }

    private List<Instance> getServiceInstances(ZooKeeper zk, String serviceName)
            throws Exception {
        String cn = clusterName.getValue();
        String path = rootServicePath.getValue() + "/" + serviceName + "/"
                + servicesSuffixPath.getValue();

        List<Instance> instances = new ArrayList<Instance>();

        log.debug("getting zk nodes for zk path: {}", path);
        for (String node : zk.getChildren(path, false)) {
            String nodePath = path + "/" + node;

            log.debug("loading zk node: {}", nodePath);
            byte[] b = zk.getData(nodePath, false, zk.exists(nodePath, true));
            String json = new String(b);
            log.debug("zk data for [{}]: {}", nodePath, json);

            Service serviceInstance = om.readValue(json, Service.class);

            if (isValidHystrixInstance(serviceInstance)) {
                instances.add(new Instance(serviceInstance.getHost() + ":"
                        + serviceInstance.getPort(), cn, true));
            }
        }

        return instances;
    }

    private boolean isValidHystrixInstance(Service service) {
        HttpGet get = new HttpGet(String.format("%s://%s:%s%s",
                protocol.get(),
                service.getHost(),
                service.getPort(),
                healthPath.get().startsWith("/") ? healthPath.get() : "/" + healthPath.get()));
        
        log.debug("checking if service {} supports hystrix at health url {}", service, get.getURI());
        try {
            HttpResponse response = httpClient.execute(get);
            if (response.getStatusLine().getStatusCode() == 200) {
                Health health = om.readValue(EntityUtils.toString(response.getEntity()), Health.class);
                return health.isHystrix();
            } else {
                log.warn("service {} health check failed: {}", response);
            }
        } catch (Exception e) {
            log.warn("error getting health data from service {} using url {}", service, get.getURI(), e);
        }
        
        return false;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.debug("event received from zk: {}", watchedEvent);
    }
}
