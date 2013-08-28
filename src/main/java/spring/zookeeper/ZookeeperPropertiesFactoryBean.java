package spring.zookeeper;

import org.springframework.beans.factory.FactoryBean;

import java.util.Properties;

public class ZookeeperPropertiesFactoryBean implements FactoryBean<Properties> {

    private SpringZookeeperUtils zookeeperUtils;
    private final Properties props = new Properties();

    public void setZookeeperUtils(SpringZookeeperUtils zookeeperUtils) {
        this.zookeeperUtils = zookeeperUtils;
    }


    public Class<?> getObjectType() {
        return Properties.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public Properties getObject() throws Exception {
        if (props.isEmpty()) {
            Properties zooProperties = zookeeperUtils.loadProperties();
            props.putAll(zooProperties);
        }
        return props;
    }

}
