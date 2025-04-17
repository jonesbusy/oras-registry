package io.github.jonesbusy;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import land.oras.ContainerRef;
import land.oras.Index;
import land.oras.Registry;

@Path("/client")
public class Client {

    @Inject
    @Named("docker")
    Registry registry;

    @GET
    @Path("/alpine")
    @Produces(MediaType.APPLICATION_JSON)
    public String index() {
        return registry.getIndex(ContainerRef.parse("library/alpine:latest")).getJson();
    }

    @GET
    @Path("/maven")
    @Produces(MediaType.APPLICATION_JSON)
    public String manifest() {
        return registry.getIndex(ContainerRef.parse("library/maven:latest")).getJson();
    }

}
