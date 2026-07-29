package net.surpin.data.arrowflight.server;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Set;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;

/**
 * Основной класс сервера Hadoop Arrow Flight SQL.
 */
public class HadoopArrowFlightServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopArrowFlightServer.class);

    private static final String DEFAULT_DATA_DIR = "/data/parquet";
    private static final int DEFAULT_PORT = 32010;
    private static final int DEFAULT_HAZELCAST_PORT = 5701;
    private static final String STATEMENT_CACHE_NAME = "statement-cache";

    private FlightServer server;
    private ParquetManager parquetManager;
    private Configuration hadoopConfig;
    private FileSystem fileSystem;
    private BufferAllocator allocator;
    private HazelcastInstance hazelcastInstance;
    private IMap<String, Serializable> statementCache;

    /**
     * Запуск сервера.
     */
    public void start(String... args) {
        String dataDirectory = getArgValue(args, "--data-dir", DEFAULT_DATA_DIR);
        int port = Integer.parseInt(getArgValue(args, "--port", String.valueOf(DEFAULT_PORT)));
        String hosts = getArgValue(args, "--hosts", "0.0.0.0");
        String localhost = getArgValue(args, "--localhost", "localhost");

        int hazelcastPort = Integer.parseInt(getArgValue(args, "--hazelcast-port", String.valueOf(DEFAULT_HAZELCAST_PORT)));

        LOGGER.info("Запуск Hadoop Arrow Flight SQL сервера...");
        LOGGER.info("Data Directory: {}", dataDirectory);
        LOGGER.info("Hosts: {}", hosts);
        LOGGER.info("Port: {}", port);

        // Инициализация hazelcast
        setupHazelcast(hazelcastPort, localhost, hosts.split(","));
        statementCache = hazelcastInstance.getMap(STATEMENT_CACHE_NAME);

        // Инициализация конфигурации Hadoop
        this.hadoopConfig = new Configuration();
        hadoopConfig.setInt("io.file.buffer.size", 131072); // 128KB
        hadoopConfig.setBoolean("dfs.client.read.shortcircuit", true);
        hadoopConfig.setBoolean("dfs.client.read.shortcircuit.skip.checksum", false);

        try {
            this.fileSystem = FileSystem.get(hadoopConfig);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (UnsupportedOperationException e) {
            // Java 21+: Subject.getSubject(AccessControlContext) was removed; UGI cannot obtain
            // current user via the legacy security-manager API. Fall back to a direct LocalFileSystem
            // (no UGI lookup needed) for local-filesystem data directories.
            LOGGER.warn("Subject.getSubject() not supported (Java 21+) – falling back to LocalFileSystem", e);
            try {
                org.apache.hadoop.fs.LocalFileSystem localFs = new org.apache.hadoop.fs.LocalFileSystem();
                localFs.initialize(java.net.URI.create("file:///"), hadoopConfig);
                this.fileSystem = localFs;
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        }

        // Инициализация менеджера Parquet файлов
        parquetManager = new ParquetManager(fileSystem, dataDirectory, localhost);

        // Создание локации (адреса сервера)
        Location location = Location.forGrpcInsecure(localhost, port);

        // Запуск сервера
        try {
            // Создание allocator для Arrow
            System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME, DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
            allocator = new RootAllocator(Long.MAX_VALUE);

            // Инициализация Flight SQL сервера
            HadoopFlightSqlService sqlService = new HadoopFlightSqlService(location, parquetManager, allocator, hazelcastInstance);

            server = FlightServer.builder(allocator, location, sqlService).build();
            server.start();

            LOGGER.info("Сервер Arrow Flight SQL запущен на {}", location);
            LOGGER.info("Data Directory: {}", dataDirectory);

            // Ожидание завершения (сервер работает в фоновом потоке)
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Остановка сервера...");
                stop();
            }));

            // Блокируем основной поток
            try {
                server.awaitTermination();
            } catch (InterruptedException e) {
                LOGGER.info("Ожидание прервано");
            }

        } catch (Exception e) {
            LOGGER.error("Ошибка при запуске сервера", e);
            System.exit(1);
        }
    }

    protected void setupHazelcast(int hazelcastPort, String localhost, String... hosts) {
        Config config = new Config();
        NetworkConfig network = config.getNetworkConfig();
        network.setPort(hazelcastPort);
//        network.getInterfaces().addInterface(localhost).setEnabled(true);
        network.setPortAutoIncrement(false);
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        Arrays.stream(hosts).forEach(tcpIpConfig::addMember);
        tcpIpConfig.setEnabled(true);

        LOGGER.info("Hosts: {}", Arrays.asList(hosts));
        LOGGER.info("Hazelcast config: {}", config);

        hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        LOGGER.info("Waiting for all {} nodes to connect", hosts.length);
        Set<Member> members;
        do {
            members = hazelcastInstance.getCluster().getMembers();
            LOGGER.info("Connected: {} of {} nodes", members.size(), hosts.length);
        } while (members.size() < hosts.length);
        LOGGER.info("All {} nodes connected. Initializing...", hosts.length);
    }

    /**
     * Остановка сервера.
     */
    public void stop() {
        if (server != null) {
            server.shutdown();
            LOGGER.info("Flight SQL сервер остановлен");
        }
        if (allocator != null) {
            allocator.close();
            LOGGER.info("Arrow allocator закрыт");
        }
    }

    private String getArgValue(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(key) && i + 1 < args.length) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    public static void main(String... args) throws Exception {
        HadoopArrowFlightServer server = new HadoopArrowFlightServer();
        server.start(args);
//        server.start("--data-dir", "/Users/16713217/Documents/GigaCodeCLI/hadoop-arrow-flight/src/test/resources/test_db/");
    }
}
