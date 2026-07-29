package net.surpin.data.arrowflight.client;

import com.google.protobuf.Any;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Arrays;

/**
 * Describes the data-structure of Client for communicating with remote flight service
 */
public final class Client implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    //the factory
    private static final ClientIncomingAuthHeaderMiddleware.Factory factory = new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
    //the existing objects of client
    private static final java.util.Map<String, Client> clients = new java.util.HashMap<>();

    //the flight client
    private final FlightClient client;
    //the flight-sql client
    private final FlightSqlClient sqlClient;
    //the token for calls
    private final CredentialCallOption bearerToken;

    /**
     * Get the underlying FlightClient (for streaming reads)
     */
    public FlightClient getFlightClient() {
        return this.client;
    }

    /**
     * Get the bearer token credential (for streaming reads)
     */
    public CredentialCallOption getBearerToken() {
        return this.bearerToken;
    }

    //the buffer
    private final BufferAllocator allocator;
    //the connection string to identify the client
    private final String connectionString;

    /**
     * Construct a Client object
     * @param client - the client object of the flight service
     * @param bearerToken - the credential token
     * @param connectionString - the connection string to identify the client
     */
    private Client(FlightClient client, CredentialCallOption bearerToken, String connectionString, BufferAllocator allocator) {
        this.client = client;
        this.sqlClient = new FlightSqlClient(this.client);
        this.bearerToken = bearerToken;

        this.connectionString = connectionString;
        this.allocator = allocator;
    }

    /**
     * Fetch meta-data of all related end-points by a query
     * @param query - the query submitted to remote flight-service
     * @return - the schema and end-points for the query
     */
    public QueryEndpoints getQueryEndpoints(String query) {
//        FlightInfo fi = this.client.getInfo(FlightDescriptor.command(query.getBytes(StandardCharsets.UTF_8)), this.bearerToken);
        final FlightSql.CommandStatementQuery.Builder builder = FlightSql.CommandStatementQuery.newBuilder().setQuery(query);
        final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());

        FlightInfo fi = this.client.getInfo(descriptor, this.bearerToken);
        LOGGER.info("Client.getQueryEndpoints('{}'): got endpoints \n{}", query, fi.getEndpoints());
        Endpoint[] endpoints = fi.getEndpoints().stream().map(ep -> new Endpoint(ep.getLocations().stream().map(Location::getUri).toArray(URI[]::new), ep.getTicket().getBytes())).toArray(Endpoint[]::new);
        LOGGER.info("Client.getQueryEndpoints('{}'): return endpoints \n{}", query, Arrays.asList(endpoints));
        return new QueryEndpoints(fi.getSchema(), endpoints);
    }

    /**
     * Fetch rows from the end-point
     * @param ep - the end-point
     * @param schema - the schema of rows from the end-point
     * @return - row set from the end-point
     */
    public RowSet fetch(Endpoint ep, Schema schema) {
        RowSet rs = new RowSet(schema);
        Field[] fields = Field.from(schema);
        FlightEndpoint fep = new FlightEndpoint(new Ticket(ep.getTicket()), Arrays.stream(ep.getURIs()).map(Location::new).toArray(Location[]::new));
        try {
            try (FlightStream stream = this.client.getStream(fep.getTicket(), this.bearerToken)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    FieldVector[] fs = root.getFieldVectors().stream().map(fv -> FieldVector.fromArrow(fv, Field.find(fields, fv.getName()), root.getRowCount())).toArray(FieldVector[]::new);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        RowSet.Row row = new RowSet.Row();
                        for (FieldVector f : fs) {
                            row.add((f.getValues())[i]);
                        }
                        rs.add(row);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rs;
    }

    /**
     * Fetch rows from all end-points
     * @param qEndpoints - the query end-points
     * @return - all rows from the end-points
     */
    public RowSet fetch(QueryEndpoints qEndpoints) {
        RowSet rs = new RowSet(qEndpoints.getSchema());
        Arrays.stream(qEndpoints.getEndpoints()).forEach(ep -> rs.add(fetch(ep, qEndpoints.getSchema())));
        return rs;
    }

    /**
     * Fetch rows from the end-point in a streaming fashion.
     * Each batch from the Flight stream is passed to the callback immediately,
     * without accumulating all rows in memory.
     */
    public void fetchStreaming(Endpoint ep, Schema schema, BatchCallback callback) {
        Field[] fields = Field.from(schema);
        FlightEndpoint fep = new FlightEndpoint(new Ticket(ep.getTicket()), Arrays.stream(ep.getURIs()).map(Location::new).toArray(Location[]::new));
        try {
            try (FlightStream stream = this.client.getStream(fep.getTicket(), this.bearerToken)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    boolean shouldContinue = callback.onBatch(root, fields);
                    if (!shouldContinue) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fetch rows from all end-points in a streaming fashion.
     */
    public void fetchStreaming(QueryEndpoints qEndpoints, BatchCallback callback) {
        for (Endpoint ep : qEndpoints.getEndpoints()) {
            fetchStreaming(ep, qEndpoints.getSchema(), callback);
        }
    }

    /**
     * Callback interface for streaming batch processing.
     */
    @FunctionalInterface
    public interface BatchCallback {
        /**
         * Process a single batch of data.
         * @param root the current VectorSchemaRoot with data
         * @param fields the field definitions for the schema
         * @return true to continue, false to stop streaming
         */
        boolean onBatch(VectorSchemaRoot root, Field[] fields);
    }

    /**
     * Execute a literal SQL statement
     * @param stmt - the literal sql-statement
     */
    public long execute(String stmt) {
        FlightInfo fi = this.sqlClient.execute(stmt, this.bearerToken);
        LOGGER.info("Client.execute('{}'): got endpoints \n{}", stmt, fi.getEndpoints());
        long count = 0;
        for (FlightEndpoint endpoint: fi.getEndpoints()) {
            try {
                try (FlightStream stream = this.client.getStream(endpoint.getTicket(), this.bearerToken)) {
                    while (stream.next()) {
                        VectorSchemaRoot root = stream.getRoot();
                        count += root.getRowCount();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return count;
    }

    /**
     * Truncate the target table
     * @param table - the name of the table
     */
    public void truncate(String table) {
        this.execute(String.format("truncate table %s", table));
    }

    /**
     * Get a prepared-statement for a query
     * @param query - the query being used for update
     * @return - the prepared sql-statement for down-stream operations
     */
    public FlightSqlClient.PreparedStatement getPreparedStatement(String query) {
        return this.sqlClient.prepare(query, this.bearerToken);
    }

    /**
     * Execute a prepared-statement
     * @param preparedStmt - the prepared-statement being executed.
     * @return - the number of rows affected.
     */
    public long executeUpdate(FlightSqlClient.PreparedStatement preparedStmt) {
        return preparedStmt.executeUpdate(this.bearerToken);
    }

    /**
     * Close the connection
     */
    @Override
    public void close() {
        try {
            synchronized (Client.clients) {
                Client.clients.remove(this.connectionString);
            }
            this.client.close();

            this.allocator.getChildAllocators().forEach(BufferAllocator::close);
            AutoCloseables.close(this.allocator);
        } catch (Exception ex) {
            LoggerFactory.getLogger(this.getClass()).warn(ex.getMessage() + Arrays.toString(ex.getStackTrace()));
        }
    }

    /**
     * Get a client object
     * @param config - the connection configuration for establishing connections to remote flight service
     * @return - the client object
     */
    /**
     * Default max allocation per client: 2GB.
     * Can be overridden via {@link Configuration}.
     */
    private static final long DEFAULT_MAX_ALLOCATION = 2L * 1024 * 1024 * 1024; // 2GB

    public static synchronized Client getOrCreate(Configuration config) {
        String cs = config.getConnectionString();
        if (!Client.clients.containsKey(cs)) {
            long maxAllocation = config.getAllocationLimit() > 0
                    ? config.getAllocationLimit()
                    : DEFAULT_MAX_ALLOCATION;

            final BufferAllocator allocator = new RootAllocator(maxAllocation);
            final FlightClient client = Client.create(config, allocator);

            final CallHeaders callHeaders = new FlightCallHeaders();
            if (config.getDefaultSchema() != null && config.getDefaultSchema().length() > 0) {
                callHeaders.insert("SCHEMA", config.getDefaultSchema());
            }
            if (config.getRoutingTag() != null && config.getRoutingTag().length() > 0) {
                callHeaders.insert("ROUTING_TAG", config.getRoutingTag());
            }
            if (config.getRoutingQueue() != null && config.getRoutingQueue().length() > 0) {
                callHeaders.insert("ROUTING_QUEUE", config.getRoutingQueue());
            }
            final HeaderCallOption clientProperties = (callHeaders.keys().size() > 0) ? new HeaderCallOption(callHeaders) : null;

            Client.clients.put(cs, new Client(client, authenticate(client, config.getUser(), config.getPassword(), config.getBearerToken(), clientProperties), cs, allocator));
        }
        return Client.clients.get(cs);
    }

    //Create a client object with the service configuration
    private static FlightClient create(Configuration config, BufferAllocator allocator) {
        FlightClient.Builder builder = FlightClient.builder().allocator(allocator);
        if (config.getTlsEnabled()) {
            if (config.getTruststoreJks() == null || config.getTruststoreJks().isEmpty()) {
                builder.location(Location.forGrpcTls(config.getFlightHost(), config.getFlightPort())).useTls().verifyServer(config.verifyServer());
            } else {
                builder.location(Location.forGrpcTls(config.getFlightHost(), config.getFlightPort())).useTls().trustedCertificates(new ByteArrayInputStream(config.getCertificateBytes()));
            }
        } else {
            builder.location(Location.forGrpcInsecure(config.getFlightHost(), config.getFlightPort()));
        }
        return (config.getPassword() != null && config.getPassword().length() > 0) ? builder.intercept(Client.factory).build() : builder.build();
    }
    //Authenticate with user & password to obtain the credential token-ticket
    private static CredentialCallOption authenticate(FlightClient client, String user, String password, String bearerToken, HeaderCallOption clientProperties) {
        final java.util.List<CallOption> callOptions = new java.util.ArrayList<>();

        LOGGER.info("Client.authenticate: clientProperties={}", clientProperties);
        if (clientProperties != null) {
            callOptions.add(clientProperties);
        }
        boolean usePassword = (password != null && password.length() > 0);
        callOptions.add(new CredentialCallOption(usePassword ? new BasicAuthCredentialWriter(user, password) : new BearerCredentialWriter(bearerToken)));
        LOGGER.info("Client.authenticate: callOptions={}", callOptions);
        client.handshake(callOptions.toArray(new CallOption[0]));
        LOGGER.info("Client.authenticate: handshake finished");
        return usePassword ? Client.factory.getCredentialCallOption() : (CredentialCallOption) callOptions.get(0);
    }
}
