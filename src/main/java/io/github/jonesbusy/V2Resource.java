package io.github.jonesbusy;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import land.oras.Layer;
import land.oras.LayoutRef;
import land.oras.OCILayout;
import land.oras.utils.JsonUtils;
import org.jboss.resteasy.reactive.RestPath;
import org.jboss.resteasy.reactive.RestQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/v2")
public class V2Resource {

    private static final Logger LOG = LoggerFactory.getLogger(V2Resource.class);

    /**
     * end-1
     * @return 200
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String end1() {
        return "OK";
    }

    /**
     * end-2
     */
    @GET
    @Path("{name}/blobs/{digest}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response end2(@RestPath("name") String name, @RestPath String digest) {
        try {
            OCILayout ociLayout = OCILayout.Builder.builder().defaults(java.nio.file.Path.of(name)).build();
            LayoutRef layoutRef = LayoutRef.parse("%s@%s".formatted(ociLayout.getPath(), digest));
            return Response.ok(ociLayout.getBlob(layoutRef)).build();
        }
        catch (Exception e) {
            LOG.warn("Failed to get blob", e);
            return Response.status(Response.Status.NOT_FOUND).entity(e.getMessage()).build();
        }
    }

    /**
     * Monolithic upload
     */
    @POST
    @Path("{name}/blobs/uploads")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response end4b(@RestPath("name") String name, @RestQuery("digest") String digest, byte[] body) {
        try {
            OCILayout ociLayout = OCILayout.Builder.builder().defaults(java.nio.file.Path.of(name)).build();
            LayoutRef layoutRef = LayoutRef.parse("%s@%s".formatted(ociLayout.getPath(), digest));
            Layer layer = ociLayout.pushBlob(layoutRef, body);
            return Response.ok(JsonUtils.toJson(layer)).build();
        }
        catch (Exception e) {
            LOG.warn("Failed to create upload", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }

}
