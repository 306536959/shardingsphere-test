package org.apache.shardingsphere.proxy;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.authority.yaml.config.YamlUserConfiguration;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.YamlGlobalRuleConfiguration;
import org.apache.shardingsphere.proxy.arguments.BootstrapArguments;
import org.apache.shardingsphere.proxy.backend.config.YamlProxyConfiguration;
import org.apache.shardingsphere.proxy.backend.config.yaml.YamlProxyDataSourceConfiguration;
import org.apache.shardingsphere.proxy.backend.config.yaml.YamlProxyServerConfiguration;
import org.apache.shardingsphere.proxy.frontend.CDCServer;
import org.apache.shardingsphere.proxy.frontend.ShardingSphereProxy;
import org.apache.shardingsphere.proxy.frontend.ssl.ProxySSLContext;
import org.apache.shardingsphere.proxy.initializer.BootstrapInitializer;
import org.apache.shardingsphere.proxy.initializer.ConfigFetcher;
import org.apache.shardingsphere.proxy.initializer.ConfigParser;

import org.apache.shardingsphere.proxy.initializer.ConfigWatcher;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Bootstrap {

    private static final Logger log = LoggerFactory.getLogger(Bootstrap.class);

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(final String[] args) throws IOException, SQLException, ParseException {
        BootstrapArguments bootstrapArgs = new BootstrapArguments(args);
        String configUrl = "https://apifoxmock.com/m2/5550113-5227108-default/250637533";
        YamlProxyConfiguration yamlConfig = loadConfigFromHttp(configUrl);
        YamlProxyDataSourceConfiguration dataSourceConfig = yamlConfig.getDatabaseConfigurations().get("test").getDataSources().get("ds_0");
        YamlProxyServerConfiguration serverConfig = yamlConfig.getServerConfiguration();

//        URL url = new URL("http://localhost:8080/sse");
//        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
//        connection.setRequestMethod("GET");
//        connection.setRequestProperty("Accept", "text/event-stream");
//
//        BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
//        String line;
//        while ((line = reader.readLine()) != null) {
//            if (line.startsWith("data:")) {
//                String data = line.substring(5);
//                System.out.println("Received message from server: " + data);
//            }
//        }

        if (!Objects.isNull(serverConfig.getAuthority())){
        Collection<YamlUserConfiguration> users = serverConfig.getAuthority().getUsers();
        for (YamlUserConfiguration user : users) {
            log.info("数据库账号:{},数据库密码:{}", user.getUser(), user.getPassword());
        }
        log.info("数据库账号:{},数据库密码:{}", dataSourceConfig.getUsername(), dataSourceConfig.getPassword());
        }
        int port = bootstrapArgs.getPort().orElseGet(() -> new ConfigurationProperties(yamlConfig.getServerConfiguration().getProps()).getValue(ConfigurationPropertyKey.PROXY_DEFAULT_PORT));
        List<String> addresses = bootstrapArgs.getAddresses();
        checkPort(addresses, port);
        BootstrapInitializer bootstrapInitializer = new BootstrapInitializer();
        bootstrapInitializer.init(yamlConfig, port, bootstrapArgs.isForce());

        // 启动配置监听器
        ConfigWatcher configWatcher = new ConfigWatcher(configUrl, bootstrapInitializer);
        configWatcher.startWatching(60000); // 每分钟检查一次配置
        configWatcher.printTimer(6000);
        Optional.ofNullable((Integer) yamlConfig.getServerConfiguration().getProps().get(ConfigurationPropertyKey.CDC_SERVER_PORT.getKey()))
                .ifPresent(optional -> new Thread(new CDCServer(addresses, optional)).start());
        ProxySSLContext.init();
        ShardingSphereProxy proxy = new ShardingSphereProxy();
        bootstrapArgs.getSocketPath().ifPresent(proxy::start);
        proxy.start(port, addresses);
    }

    private static YamlProxyConfiguration loadConfigFromHttp(String configUrl) throws IOException {
        String jsonString = ConfigFetcher.fetchConfig(configUrl);
        return ConfigParser.parseConfig(jsonString);
    }

    private static void checkPort(final List<String> addresses, final int port) throws IOException {
        for (String each : addresses) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.bind(new InetSocketAddress(each, port));
            }
        }
    }

//    // 注册
//    public static void register() {
//        try {
//            new BootstrapInitializer().register();
//            System.out.println("register success");
//        } catch (Exception e) {
//            System.out.println("register fail");
//        }
//    }
}
