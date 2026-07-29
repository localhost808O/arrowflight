package net.surpin.data.arrowflight.client;

import java.io.Serializable;

/**
 * Describes the format of a field-vector
 */
public class FieldVector implements Serializable {
    //the field information
    private final Field field;
    //the values in the vector
    private final Object[] values;

    /**
     * Construct a FieldVector
     * @param name - the nanem of the field
     * @param type - the data type of the field
     * @param values - the objects in the vector
     */
    public FieldVector(String name, FieldType type, Object[] values) {
        this.field = new Field(name, type);
        this.values = values;
    }

    /**
     * Get the field
     * @return - the field for this vector
     */
    public Field getField() {
        return this.field;
    }

    /**
     * Get the data in the vector
     * @return - data in the vector
     */
    public Object[] getValues() {
        return this.values;
    }

    /**
     * Convert an arrow-FieldVector into a custom FieldVector
     * @param vector - the arrow field-vector
     * @param type - the data type of the field for the vector
     * @param rowCount - number of rows in the vector
     * @return - an instance of the custom FieldVector
     */
    public static FieldVector fromArrow(org.apache.arrow.vector.FieldVector vector, FieldType type, int rowCount) {
        return ArrowConversion.getOrCreate().convert(vector, type, rowCount);
    }
}
