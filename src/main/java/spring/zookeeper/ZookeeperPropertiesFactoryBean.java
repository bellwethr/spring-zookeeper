package spring.zookeeper;

import com.google.common.io.Closeables;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.FactoryBeanNotInitializedException;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.env.Environment;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class ZookeeperPropertiesFactoryBean extends AbstractFactoryBean<Properties> implements EnvironmentAware,
        PriorityOrdered {

    private Logger logger = LoggerFactory.getLogger(ZookeeperPropertiesFactoryBean.class);

    private Environment environment;

    private int order = 0;

    private CuratorFramework curator = null;

    private String connectString;
    private String path;
    private String resolvedConnectString;
    private String resolvedPath;

    public Class<?> getObjectType() {
        return Map.class;
    }

    public boolean isSingleton() {
        return false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        resolvedConnectString = environment.resolvePlaceholders(connectString);
        resolvedPath = environment.resolvePlaceholders(path);

        if (resolvedConnectString == null) {
            throw new FactoryBeanNotInitializedException(
                    "A connectString is required--cannot get properties from ZooKeeper");
        }

        if (resolvedPath == null) {
            throw new FactoryBeanNotInitializedException("A path is required--cannot get properties from ZooKeeper");
        }

        logger.info("Creating CuratorFramework client for [{}]", resolvedConnectString);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(10, 100);
        curator = CuratorFrameworkFactory.newClient(resolvedConnectString, retryPolicy);
        curator.start();
    }

    @Override
    public void destroy() throws Exception {
        curator.close();
    }

    @Override
    protected Properties createInstance() throws Exception {
        InputStream in = null;
        try {
            if (curator.checkExists().forPath(resolvedPath) == null) {
                throw new BeanInitializationException("Failed to load properties from non-existent path ["
                        + resolvedPath + "] for zookeeper instance [" + resolvedConnectString + "]");
            }
            logger.info("Loading properties from ZooKeeper instance at [{}] with path [{}]", resolvedConnectString,
                    resolvedPath);
            in = new ByteArrayInputStream(curator.getData().forPath(resolvedPath));
            Properties properties = new Properties();
            properties.load(in);
            for (Entry<Object, Object> entry : properties.entrySet()) {
                logger.debug("Loaded property [{} = {}]", entry.getKey(), entry.getValue());
            }
            return properties;
        } catch (Exception e) {
            logger.error("Error attempting to load properties from ZooKeeper", e);
            throw new IllegalStateException("Could not load properties from ZooKeeper", e);
        } finally {
            Closeables.closeQuietly(in);
        }
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

}