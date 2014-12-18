package fitpay.turbine.discovery.nerve;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

public class ZooKeeperNerveInstanceDiscovery implements InstanceDiscovery, Watcher {
    private static Logger log = LoggerFactory.getLogger(ZooKeeperNerveInstanceDiscovery.class);

    private final static DynamicStringProperty zkHosts = DynamicPropertyFactory.getInstance().getStringProperty("turbine.ZkDiscovery.zkHosts", null);
    private final static DynamicIntProperty timeout = DynamicPropertyFactory.getInstance().getIntProperty("turbine.ZkDiscovery.timeout", 3000);

    private final static DynamicStringProperty rootServicePath = DynamicPropertyFactory.getInstance().getStringProperty("turbine.ZkDiscovery.rootServicePath", "/nerve/services");
    private final static DynamicStringProperty servicesSuffixPath = DynamicPropertyFactory.getInstance().getStringProperty("turbine.ZkDiscovery.servicesSuffixPath", "services");

    private final static DynamicStringProperty clusterName = DynamicPropertyFactory.getInstance().getStringProperty("turbine.ZkDiscovery.clusterName", "Default");

    private final static ObjectMapper om = new ObjectMapper();

    @Override
    public Collection<Instance> getInstanceList() throws Exception {
        Collection<Instance> instances = new ArrayList<Instance>();

        log.debug("connecting to zookeeper hosts: {}", zkHosts.getValue());
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(zkHosts.getValue(), timeout.getValue(), this);

            for (String serviceName : zk.getChildren(rootServicePath.getValue(), false)) {
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

    private List<Instance> getServiceInstances(ZooKeeper zk, String serviceName) throws Exception {
        String cn = clusterName.getValue();
        String path = rootServicePath.getValue() + "/" + serviceName + "/" + servicesSuffixPath.getValue();

        List<Instance> instances = new ArrayList<Instance>();

        log.debug("getting zk nodes for zk path: {}", path);
        for (String node : zk.getChildren(path, false)) {
            String nodePath = path + "/" + node;

            log.debug("loading zk node: {}", nodePath);
            byte[] b = zk.getData(nodePath, false, zk.exists(nodePath, true));
            String json = new String(b);
            log.debug("zk data for [{}]: {}", nodePath, json);

            @SuppressWarnings("unchecked")
			Map<String, String> m = om.readValue(json, Map.class);
            instances.add(new Instance(m.get("host") + ":" + m.get("port"), cn, true));
        }

        return instances;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.debug("event received from zk: {}", watchedEvent);
    }
}
