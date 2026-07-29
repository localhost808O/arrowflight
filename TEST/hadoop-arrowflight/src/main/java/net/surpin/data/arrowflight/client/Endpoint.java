package net.surpin.data.arrowflight.client;

import java.io.Serializable;
import java.net.URI;

/**
 * Describes the data-structure of a flight end-point for connections
 */
public class Endpoint implements Serializable {
    //the URIs of the end-point
    private final URI[] uris;
    //the ticket for connecting to the end-point
    private final byte[] ticket;

    /**
     * Construct an end-point
     * @param uris - the URIs of the end-point
     * @param ticket - the ticket for connecting to the end-point
     */
    public Endpoint(URI[] uris, byte[] ticket) {
        this.uris = uris;
        this.ticket = ticket;
    }

    /**
     * Get the URIs of the end-point
     * @return - the URIs of the end-point
     */
    public URI[] getURIs() {
        return this.uris;
    }

    /**
     * Get the ticket of the end-point
     * @return - the ticket of the end-point
     */
    public byte[] getTicket() {
        return this.ticket;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getName());
        sb.append(" {");
        sb.append(" URIs: ");
        if (uris != null) {
            for (URI uri : uris) {
                sb.append(uri);
                sb.append(" ");
            }
        }
        sb.append("}");

        return sb.toString();
    }
}
