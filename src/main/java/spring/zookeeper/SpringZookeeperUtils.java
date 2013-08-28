package spring.zookeeper;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.support.ResourcePropertySource;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Exception;import java.lang.IllegalStateException;import java.lang.Object;import java.lang.RuntimeException;import java.lang.String;import java.lang.System;import java.util.Properties;

public class SpringZookeeperUtils {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private final CuratorFramework curator;


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String environment;
    private String projectArtifactId;
    private String projectVersion;
    private Logger logger;

    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public SpringZookeeperUtils() throws IOException {
        String zkConnectString = System.getProperty("zookeeper.connect");
        environment = System.getProperty("environment");
        logger = LoggerFactory.getLogger(SpringZookeeperUtils.class);

        if (zkConnectString == null || environment == null) {
            throw new RuntimeException(String.format("'zookeeper.connect' (%s) or 'environment' (%s) is not set as a system property", zkConnectString, environment));
        }
        logger.info("Attempting to construct CuratorFramework instance");
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(10, 100);
        curator = CuratorFrameworkFactory.newClient(zkConnectString, retryPolicy);
        curator.start();
    }


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    public static void main(String args[]) throws Exception {
        System.getProperties().setProperty("zookeeper.connect", "localhost:2181");
        System.getProperties().setProperty("environment", "local");

        SpringZookeeperUtils zookeeperUtils = new SpringZookeeperUtils();

        Properties props = new Properties();
        props.put("foo", "bar");
        props.put("messaging.kafka.brokerList","zookeeper_injected_brokerlist");
        props.put("messaging.zookeeper.nodeList","zookeeper_injected_nodelist");
        zookeeperUtils.publishProperties(props);
        Properties loadedProps = zookeeperUtils.loadProperties();
        for (Object key : loadedProps.keySet()) {
            System.out.println("key: " + key + ", value: " + loadedProps.get(key));
        }
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public void publishProperties(Properties propertiesToPublish) {
        ByteArrayOutputStream baos = null;

        try {
            populateProjectProperties();
            String path = "/config/" + environment + "/" + projectArtifactId + "/" + projectVersion;
            Stat exists = curator.checkExists().forPath(path);
            if (exists == null) {
                curator.create().creatingParentsIfNeeded().forPath(path);
            }

            baos = new ByteArrayOutputStream();
            propertiesToPublish.store(baos, null);
            curator.setData().forPath("/config/" + environment + "/" + projectArtifactId + "/" + projectVersion, baos.toByteArray());
        } catch (IOException ie) {
            logger.error("Could not publish properties to Zookeeper", ie);
        } catch (Exception e) {
            logger.error("Could not publish properties to Zookeeper", e);
        } finally {
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException e) {
                    logger.error("Error shutting down output stream", e);
                }
            }
        }
    }


    public Properties loadProperties() {
        logger.trace("Attempting to fetch properties from zookeeper");

        try {
            populateProjectProperties();
            Properties properties = populatePropertiesFromZooKeeper();
            return properties;
        } catch (IOException e) {
            logger.error("IO error attempting to load properties from ZooKeeper", e);
            throw new IllegalStateException("Could not load ZooKeeper configuration");
        } catch (Exception e) {
            logger.error("Error attempting to load properties from ZooKeeper", e);
            throw new IllegalStateException("Could not load properties from ZooKeeper", e);
        }
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getEnvironment() {
        return environment;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    /**
     * Populate the Maven artifact name and version from a property file that
     * should be on the classpath, with values entered via Maven filtering.
     * <p/>
     *
     * @throws IOException
     */
    private void populateProjectProperties() throws IOException {
        logger.trace("Attempting to get project name and version from properties file");

        try {
            ResourcePropertySource projectProps = new ResourcePropertySource("project.properties");
            this.projectArtifactId = (String) projectProps.getProperty("project.artifactId");
            this.projectVersion = (String) projectProps.getProperty("project.version");
        } catch (IOException e) {
            logger.error("IO error trying to find project name and version, in order to get properties from ZooKeeper");
        }
    }


    /**
     * Do the actual loading of properties.
     *
     * @return
     * @throws Exception
     * @throws IOException
     */
    private Properties populatePropertiesFromZooKeeper() throws Exception, IOException {
        logger.debug("Attempting to get properties from ZooKeeper");
        InputStream in = null;
        try {
            String path = "/config/" + environment + "/" + projectArtifactId + "/" + projectVersion;
            Stat exists = curator.checkExists().forPath(path);
            if (exists == null) {
                curator.create().creatingParentsIfNeeded().forPath(path);
                return new Properties();
            }
            byte[] bytes = curator.getData().forPath(path);
            in = new ByteArrayInputStream(bytes);
            Properties properties = new Properties();
            properties.load(in);

            return properties;
        } catch (KeeperException.NoNodeException e) {

            logger.error(String.format("Could not load application configuration from ZooKeeper as no node existed for /config/%s/%s/%s", environment, projectArtifactId, projectVersion));
            throw e;
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
}
