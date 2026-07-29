package net.surpin.data.arrowflight.client;

import java.io.Serializable;

/**
 * Describes the data structure for pushed-down aggregation
 */
public class PushAggregation implements Serializable {
    //pushed-down Aggregate-Columns (expressions)
    private String[] columnExpressions = null;
    //pushed-down GroupBy-Columns
    private String[] groupByColumns = null;

    /**
     * Push down aggregation of columns
     *   select max(age), sum(distinct amount) from table where ...
     * @param columnExpressions - the collection of aggregation expressions
     */
    public PushAggregation(String[] columnExpressions) {
        this.columnExpressions = columnExpressions;
    }

    /**
     * Push down aggregation with group by columns
     *   select max(age), sum(amount) from table where ... group by gender
     * @param columnExpressions - the collection of aggregation expressions
     * @param groupByColumns - the columns in group by
     */
    public PushAggregation(String[] columnExpressions, String[] groupByColumns) {
        this(columnExpressions);
        this.groupByColumns = groupByColumns;
    }

    /**
     * Return the collection of aggregation expressions
     * @return - the expressions
     */
    public String[] getColumnExpressions() {
        return this.columnExpressions;
    }

    /**
     * The columns for group-by
     * @return - columns
     */
    public String[] getGroupByColumns() {
        return this.groupByColumns;
    }
}
