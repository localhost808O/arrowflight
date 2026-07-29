package net.surpin.data.arrowflight.client;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Describes a flight table
 */
public final class Table implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);

    //the name of a flight table whose data will be queried/updated
    private final String name;
    //the character for quoting columns in sql statements
    private final String columnQuote;

    //the read-statement
    private QueryStatement stmt;

    //the spark schema
    private StructType sparkSchema = null;
    //the flight schema
    private transient Schema schema = null;
    //the end-points exposed by the remote flight-service for fetching data of this table
    private Endpoint[] endpoints = new Endpoint[0];

    //the container for holding the partitioning queries
    private final java.util.List<String> partitionStmts = new java.util.ArrayList<>();

    /**
     * Construct a Table object
     * @param name - the name of the table
     * @param columnQuote - the character for quoting columns in sql statements
     */
    private Table(String name, String columnQuote) {
        this.name = name;
        this.columnQuote = columnQuote;

        this.prepareQueryStatement(null, null, null, null);
    }

    /**
     * Get the name of the table
     * @return - the name of the table
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the sql-statement for querying the table
     * @return - the physical query which will be submitted to remote flight service
     */
    public String getQueryStatement() {
        if (this.stmt == null) {
            throw new RuntimeException("The read statement is not valid.");
        }
        return this.stmt.getStatement();
    }

    /**
     * Get the partition queries
     * @return - the partition queries with each of which is submitted from a spark executor
     */
    public String[] getPartitionStatements() {
        return this.partitionStmts.toArray(new String[0]);
    }

    /**
     * Get the spark schema
     * @return - the spark schema
     */
    public StructType getSparkSchema() {
        return this.sparkSchema;
    }

    /**
     * Get the end-points
     * @return - end-points exposed by the remote flight service upon submitted query
     */
    public Endpoint[] getEndpoints() {
        return this.endpoints;
    }

    /**
     * Get the flight schema
     * @return - the flight schema
     */
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Get the character for quoting columns
     * @return - the character for quoting columns
     */
    public String getColumnQuote() {
        return this.columnQuote;
    }

    /**
     * Initialize the schema and end-points by submitting the physical query
     * @param config - the connection configuration
     */
    public void initialize(Configuration config) {
        LOGGER.info("Table.initialize(): config: {}", config);
        try {
            Client client = Client.getOrCreate(config);
            QueryEndpoints eps = client.getQueryEndpoints(this.getQueryStatement());
            LOGGER.info("Table.initialize(): endpoints: {}", eps);

            this.sparkSchema = new StructType(Arrays.stream(Field.from(eps.getSchema())).map(fs -> new StructField(fs.getName(), FieldType.toSpark(fs.getType()), true, Metadata.empty())).toArray(StructField[]::new));
            this.schema = eps.getSchema();
            this.endpoints = eps.getEndpoints();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    //Prepare the query for submitting to remote flight service
    private boolean prepareQueryStatement(PushAggregation aggregation, StructField[] fields, String filter, PartitionBehavior partitionBehavior) {
        //aggregation mode: 0 -> no aggregation; 1 -> aggregation without group-by; 2 -> aggregation with group-by
        int aggMode = 0;
        String select = "";
        String groupBy = "";
        if (aggregation != null) {
            String[] groupByFields = aggregation.getGroupByColumns();
            if (groupByFields != null && groupByFields.length > 0) {
                aggMode = 2;
                groupBy = String.join(",", groupByFields);
            } else {
                aggMode = 1;
            }
            select = String.format("select %s from %s", String.join(",", aggregation.getColumnExpressions()), this.name);
        } else if (fields != null && fields.length > 0) {
            select = String.format("select %s from %s", String.join(",", Arrays.stream(fields).map(column -> String.format("%s%s%s", this.columnQuote, column.name(), this.columnQuote)).toArray(String[]::new)), this.name);
        } else {
            select = String.format("select * from %s", this.name);
        }
        QueryStatement stmt = new QueryStatement(select, filter, groupBy);
        boolean changed = stmt.different(this.stmt);
        if (changed) {
            this.stmt = stmt;
        }

        if (aggMode == 1) {
            this.partitionStmts.clear();
        } else if (partitionBehavior != null && partitionBehavior.enabled()) {
            String where = (filter != null && !filter.isEmpty()) ? String.format("(%s) and ", filter) : "";
            BiFunction<StructField[], StructField[], StructField[]> merge = (s1, s2) -> {
                Hashtable<String, StructField> s = new Hashtable<String, StructField>();
                for (StructField sf : s1) {
                    s.put(sf.name(), sf);
                }
                for (StructField sf : s2) {
                    s.put(sf.name(), sf);
                }
                return s.values().toArray(new StructField[0]);
            };
            String[] predicates = partitionBehavior.predicateDefined() ? partitionBehavior.getPredicates()
                : partitionBehavior.calculatePredicates(this.sparkSchema == null ? fields : merge.apply(fields, this.sparkSchema.fields()));
            for (String predicate : predicates) {
                QueryStatement s = new QueryStatement(select, String.format("%s(%s)", where, predicate), groupBy);
                this.partitionStmts.add(s.getStatement());
            }
        }
        return changed;
    }

    //translate a filter to where clause
    public String toWhereClause(Filter filter) {
        StringBuilder sb = new StringBuilder();
        if (filter instanceof EqualTo) {
            EqualTo et = (EqualTo) filter;
            sb.append((et.value() instanceof Number)
                ? String.format("%s%s%s = %s", this.columnQuote, et.attribute(), this.columnQuote, et.value().toString())
                : String.format("%s%s%s = '%s'", this.columnQuote, et.attribute(), this.columnQuote, et.value().toString())
            );
        } else if (filter instanceof EqualNullSafe) {
            EqualNullSafe ens = (EqualNullSafe) filter;
            sb.append(String.format("((%s%s%s is null and %s is null) or (%s%s%s is not null and %s is not null))", this.columnQuote, ens.attribute(), this.columnQuote, ens.value(), this.columnQuote, ens.attribute(), this.columnQuote, ens.value()));
        } else if (filter instanceof LessThan) {
            LessThan lt = (LessThan) filter;
            sb.append((lt.value() instanceof Number) ? String.format("%s%s%s < %s", this.columnQuote, lt.attribute(), this.columnQuote, lt.value()) : String.format("%s%s%s < '%s'", this.columnQuote, lt.attribute(), this.columnQuote, lt.value()));
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual lt = (LessThanOrEqual) filter;
            sb.append((lt.value() instanceof Number) ? String.format("%s%s%s <= %s", this.columnQuote, lt.attribute(), this.columnQuote, lt.value()) : String.format("%s%s%s <= '%s'", this.columnQuote, lt.attribute(), this.columnQuote, lt.value()));
        } else if (filter instanceof GreaterThan) {
            GreaterThan gt = (GreaterThan) filter;
            sb.append((gt.value() instanceof Number) ? String.format("%s%s%s > %s", this.columnQuote, gt.attribute(), this.columnQuote, gt.value()) : String.format("%s%s%s > '%s'", this.columnQuote, gt.attribute(), this.columnQuote, gt.value()));
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual gt = (GreaterThanOrEqual) filter;
            sb.append((gt.value() instanceof Number) ? String.format("%s%s%s >= %s", this.columnQuote, gt.attribute(), this.columnQuote, gt.value()) : String.format("%s%s%s >= '%s'", this.columnQuote, gt.attribute(), this.columnQuote, gt.value()));
        } else if (filter instanceof And) {
            And and = (And) filter;
            sb.append(String.format("(%s and %s)", toWhereClause(and.left()), toWhereClause(and.right())));
        } else if (filter instanceof Or) {
            Or or = (Or) filter;
            sb.append(String.format("(%s or %s)", toWhereClause(or.left()), toWhereClause(or.right())));
        } else if (filter instanceof IsNull) {
            IsNull in = (IsNull) filter;
            sb.append(String.format("%s%s%s is null", this.columnQuote, in.attribute(), this.columnQuote));
        } else if (filter instanceof IsNotNull) {
            IsNotNull in = (IsNotNull) filter;
            sb.append(String.format("%s%s%s is not null", this.columnQuote, in.attribute(), this.columnQuote));
        } else if (filter instanceof StringStartsWith) {
            StringStartsWith ss = (StringStartsWith) filter;
            sb.append(String.format("%s%s%s like '%s%s'", this.columnQuote, ss.attribute(), this.columnQuote, ss.value(), "%"));
        } else if (filter instanceof StringContains) {
            StringContains sc = (StringContains) filter;
            sb.append(String.format("%s%s%s like '%s%s%s'", this.columnQuote, sc.attribute(), this.columnQuote, "%", sc.value(), "%"));
        } else if (filter instanceof StringEndsWith) {
            StringEndsWith se = (StringEndsWith) filter;
            sb.append(String.format("%s%s%s like '%s%s'", this.columnQuote, se.attribute(), this.columnQuote, "%", se.value()));
        } else if (filter instanceof Not) {
            Not not = (Not) filter;
            sb.append(String.format("not (%s)", toWhereClause(not.child())));
        } else if (filter instanceof In) {
            In in = (In) filter;
            sb.append(String.format("%s%s%s in (%s)", this.columnQuote, in.attribute(), this.columnQuote, String.join(",", Arrays.stream(in.values()).map(v -> (v instanceof Number) ? v.toString() : String.format("'%s'", v.toString())).toArray(String[]::new))));
        }
        return sb.toString();
    }

    /**
     * Probe if the pushed filter, fields and aggregation would affect the existing schema & end-points
     * @param pushedFilter - the pushed filter
     * @param pushedFields - the pushed fields
     * @param pushedAggregation - the pushed aggregation
     * @param partitionBehavior - the partitioning behavior
     * @return - true if initialization is required
     */
    public Boolean probe(String pushedFilter, StructField[] pushedFields, PushAggregation pushedAggregation, PartitionBehavior partitionBehavior) {
        if ((pushedFilter == null || pushedFilter.isEmpty()) && (pushedFields == null || pushedFields.length == 0) && pushedAggregation == null) {
            return false;
        }
        return this.prepareQueryStatement(pushedAggregation, pushedFields, pushedFilter, partitionBehavior);
    }

    /**
     * Table with name
     * @param tableName - the name of a table
     * @param columnQuote - the character for quoting columns in sql statements
     * @return - a Table object
     */
    public static Table forTable(String tableName, String columnQuote) {
        Function<String, Boolean> isQuery = (t) -> t.replaceAll("[\r|\n]", " ").trim().toLowerCase().matches("^select .+ [from]?.+");
        return new Table(isQuery.apply(tableName) ? String.format("(%s) t", tableName) : tableName, columnQuote);
    }
}
