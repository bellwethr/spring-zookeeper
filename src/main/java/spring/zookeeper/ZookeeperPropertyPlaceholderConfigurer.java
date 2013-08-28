package spring.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

import java.util.Properties;

public class ZookeeperPropertyPlaceholderConfigurer extends PropertyPlaceholderConfigurer {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private SpringZookeeperUtils zookeeperUtils;
    private Logger logger;
    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public ZookeeperPropertyPlaceholderConfigurer() {
        super();
        logger = LoggerFactory.getLogger(ZookeeperPropertyPlaceholderConfigurer.class);
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public void setZookeeperUtils(SpringZookeeperUtils zookeeperUtils) {
        this.zookeeperUtils = zookeeperUtils;
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected void processProperties(ConfigurableListableBeanFactory beanFactoryToProcess, Properties props) {
        Properties zooProperties;
        try {
            zooProperties = zookeeperUtils.loadProperties();
            props.putAll(zooProperties);
        } catch (Exception e) {
            logger.error("Unable to access properties from Zookeeper");
        }

        super.processProperties(beanFactoryToProcess, props);
    }
}
