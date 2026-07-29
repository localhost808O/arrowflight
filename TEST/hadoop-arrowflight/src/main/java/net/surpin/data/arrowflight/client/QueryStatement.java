package net.surpin.data.arrowflight.client;

import java.io.Serializable;

/**
 * The read statement for querying data from remote flight service
 */
public class QueryStatement implements Serializable {
    private final String stmt;
    private final String where;
    private final String groupBy;

    /**
     * Construct a ReadStatement
     * @param stmt - the select portion of a select-statement
     * @param where - the where portion of a select-statement
     * @param groupBy - the groupBy portion of a select-statement
     */
    public QueryStatement(String stmt, String where, String groupBy) {
        this.stmt = stmt;
        this.where = where;
        this.groupBy = groupBy;
    }

    /**
     * Check if the current ReadStatement is different from the input ReadStatement
     * @param rs - one ReadStatement to be compared
     * @return - true if they are different
     */
    public boolean different(QueryStatement rs) {
        boolean changed = (rs == null || !rs.stmt.equalsIgnoreCase(this.stmt));
        if (!changed) {
            changed = (rs.where != null) ? !rs.where.equalsIgnoreCase(this.where) : this.where != null;
        }
        if (!changed) {
            changed = (rs.groupBy != null) ? !rs.groupBy.equalsIgnoreCase(this.groupBy) : this.groupBy != null;
        }
        return changed;
    }

    /**
     * Get the whole select-statement
     * @return - the select-statement
     */
    public String getStatement() {
        return String.format("%s %s %s", this.stmt,
            (this.where != null && this.where.length() > 0) ? String.format("where %s", this.where) : "",
            (this.groupBy != null && this.groupBy.length() > 0) ? String.format("group by %s", this.groupBy) : ""
        );
    }
}
