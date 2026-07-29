package net.surpin.data.arrowflight.client;

import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Describes row batch from remote flight service
 */
public class RowSet implements Serializable {
    /**
     * The definition of Row
     */
    public static class Row implements Serializable {
        private final ArrayList<Object> data;

        public Row() {
            this.data = new ArrayList<>();
        }

        public void add(Object o) {
            this.data.add(o);
        }

        public Object[] getData() {
            return this.data.toArray(new Object[0]);
        }
    }

    //the schema of each ROw
    private final transient Schema schema;
    //the row collection
    private final ArrayList<Row> data;

    /**
     * Construct a RowSet
     * @param schema - the schema of each row in the collection
     */
    public RowSet(Schema schema) {
        this.schema = schema;
        this.data = new ArrayList<>();
    }

    /**
     * Get the schema of the RowSet
     * @return - the schema
     */
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Add one Row
     * @param row - the row to be added
     */
    public void add(Row row) {
        this.data.add(row);
    }

    /**
     * Add all rows from another RowSet
     * @param rs - the input RowSet
     */
    public void add(RowSet rs) {
        if (rs.schema != this.schema) {
            throw new RuntimeException("The schema doesn't match. Cannot add the RowSet.");
        }
        this.data.addAll(rs.data);
    }

    /**
     * Get all Rows
     * @return - all rows in the RowSet
     */
    public Row[] getData() {
        return this.data.toArray(new Row[] {});
    }
}
