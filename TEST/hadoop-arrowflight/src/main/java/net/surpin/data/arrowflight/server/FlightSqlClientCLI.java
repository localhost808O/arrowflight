package net.surpin.data.arrowflight.server;


import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class FlightSqlClientCLI implements AutoCloseable {
    public final List<CallOption> callOptions = new ArrayList<>();
    public final BufferAllocator allocator;
    private FlightSqlClient flightSqlClient;

    public FlightSqlClientCLI(BufferAllocator bufferAllocator) {
        this.allocator = bufferAllocator;
    }

    public static void main(String... args) throws Exception {
        args = new String[] {"--host", "localhost", "--port", "32010", "--command", "Execute", "--query", "select * from test_schema.test_table", "--catalog", "PARQUET_ARROW_FLIGHT_CATALOG", "--schema", "test_schema"};

        Options options = new Options();

        Option hostOption = new Option("host", "host", true, "Host to connect to");
        hostOption.setRequired(true);
        options.addOption(hostOption);

        Option portOption = new Option("port", "port", true, "Port to connect to");
        portOption.setRequired(true);
        options.addOption(portOption);

        Option commandOption = new Option("command", "command", true, "Method to run");
        commandOption.setRequired(true);
        options.addOption(commandOption);

        options.addOption("query", "query", true, "Query");
        options.addOption("catalog", "catalog", true, "Catalog");
        options.addOption("schema", "schema", true, "Schema");
        options.addOption("table", "table", true, "Table");

        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            try (org.apache.arrow.flight.sql.example.FlightSqlClientDemoApp thisApp = new org.apache.arrow.flight.sql.example.FlightSqlClientDemoApp(new RootAllocator(2147483647L))) {
                thisApp.executeApp(cmd);
            }

        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("FlightSqlClientDemoApp -host localhost -port 32010 ...", options);
            throw e;
        }
    }

    public CallOption[] getCallOptions() {
        return (CallOption[]) this.callOptions.toArray(new CallOption[0]);
    }

    public void executeApp(CommandLine cmd) throws Exception {
        String host = cmd.getOptionValue("host").trim();
        int port = Integer.parseInt(cmd.getOptionValue("port").trim());
        this.createFlightSqlClient(host, port);
        this.executeCommand(cmd);
    }

    public void executeCommand(CommandLine cmd) throws Exception {
        switch (cmd.getOptionValue("command").trim()) {
            case "Execute":
                this.exampleExecute(cmd.getOptionValue("query"));
                break;
            case "ExecuteUpdate":
                this.exampleExecuteUpdate(cmd.getOptionValue("query"));
                break;
            case "GetCatalogs":
                this.exampleGetCatalogs();
                break;
            case "GetSchemas":
                this.exampleGetSchemas(cmd.getOptionValue("catalog"), cmd.getOptionValue("schema"));
                break;
            case "GetTableTypes":
                this.exampleGetTableTypes();
                break;
            case "GetTables":
                this.exampleGetTables(cmd.getOptionValue("catalog"), cmd.getOptionValue("schema"), cmd.getOptionValue("table"));
                break;
            case "GetExportedKeys":
                this.exampleGetExportedKeys(cmd.getOptionValue("catalog"), cmd.getOptionValue("schema"), cmd.getOptionValue("table"));
                break;
            case "GetImportedKeys":
                this.exampleGetImportedKeys(cmd.getOptionValue("catalog"), cmd.getOptionValue("schema"), cmd.getOptionValue("table"));
                break;
            case "GetPrimaryKeys":
                this.exampleGetPrimaryKeys(cmd.getOptionValue("catalog"), cmd.getOptionValue("schema"), cmd.getOptionValue("table"));
                break;
            default:
                System.out.println("Command used is not valid! Please use one of: \n[\"ExecuteUpdate\",\n\"Execute\",\n\"GetCatalogs\",\n\"GetSchemas\",\n\"GetTableTypes\",\n\"GetTables\",\n\"GetExportedKeys\",\n\"GetImportedKeys\",\n\"GetPrimaryKeys\"]");
        }

    }

    public void createFlightSqlClient(String host, int port) {
        Location clientLocation = Location.forGrpcInsecure(host, port);
        this.flightSqlClient = new FlightSqlClient(FlightClient.builder(this.allocator, clientLocation).build());
    }

    private void exampleExecute(String query) throws Exception {
        this.printFlightInfoResults(this.flightSqlClient.execute(query, this.getCallOptions()));
    }

    private void exampleExecuteUpdate(String query) {
        PrintStream var10000 = System.out;
        long var10001 = this.flightSqlClient.executeUpdate(query, this.getCallOptions());
        var10000.println("Updated: " + var10001 + "rows.");
    }

    private void exampleGetCatalogs() throws Exception {
        this.printFlightInfoResults(this.flightSqlClient.getCatalogs(this.getCallOptions()));
    }

    private void exampleGetSchemas(String catalog, String schema) throws Exception {
        this.printFlightInfoResults(this.flightSqlClient.getSchemas(catalog, schema, this.getCallOptions()));
    }

    private void exampleGetTableTypes() throws Exception {
        this.printFlightInfoResults(this.flightSqlClient.getTableTypes(this.getCallOptions()));
    }

    private void exampleGetTables(String catalog, String schema, String table) throws Exception {
        this.printFlightInfoResults(this.flightSqlClient.getTables(catalog, schema, table, (List) null, false, this.getCallOptions()));
    }

    private void exampleGetExportedKeys(String catalog, String schema, String table) throws Exception {
        this.printFlightInfoResults(this.flightSqlClient.getExportedKeys(TableRef.of(catalog, schema, table), this.getCallOptions()));
    }

    private void exampleGetImportedKeys(String catalog, String schema, String table) throws Exception {
        this.printFlightInfoResults(this.flightSqlClient.getImportedKeys(TableRef.of(catalog, schema, table), this.getCallOptions()));
    }

    private void exampleGetPrimaryKeys(String catalog, String schema, String table) throws Exception {
        this.printFlightInfoResults(this.flightSqlClient.getPrimaryKeys(TableRef.of(catalog, schema, table), this.getCallOptions()));
    }

    private void printFlightInfoResults(FlightInfo flightInfo) throws Exception {
        FlightStream stream = this.flightSqlClient.getStream(((FlightEndpoint) flightInfo.getEndpoints().get(0)).getTicket(), this.getCallOptions());

        while (stream.next()) {
            try (VectorSchemaRoot root = stream.getRoot()) {
                System.out.println(root.contentToTSVString());
            }
        }

        stream.close();
    }

    public void close() throws Exception {
        this.flightSqlClient.close();
        this.allocator.close();
    }
}
