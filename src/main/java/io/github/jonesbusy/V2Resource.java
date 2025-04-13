package io.github.jonesbusy;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import land.oras.Layer;
import land.oras.LayoutRef;
import land.oras.OCILayout;
import land.oras.utils.Const;
import land.oras.utils.JsonUtils;
import org.jboss.resteasy.reactive.RestPath;
import org.jboss.resteasy.reactive.RestQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Files;
import java.util.UUID;


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
    public Response end2Get(@RestPath("name") String name, @RestPath String digest) {
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
     * end-2
     */
    @HEAD
    @Path("{name}/blobs/{digest}")
    public Response end2Head(@RestPath("name") String name, @RestPath String digest) {
        try {
            OCILayout ociLayout = OCILayout.Builder.builder().defaults(java.nio.file.Path.of(name)).build();
            LayoutRef layoutRef = LayoutRef.parse("%s@%s".formatted(ociLayout.getPath(), digest));
            byte[] blob = ociLayout.getBlob(layoutRef);
            return Response.ok().header(Const.CONTENT_LENGTH_HEADER, blob.length).build();
        }
        catch (Exception e) {
            LOG.warn("Failed to get blob", e);
            return Response.status(Response.Status.NOT_FOUND).entity(e.getMessage()).build();
        }
    }

    /**
     * Monolithic upload
     */
    @PUT
    @Path("{name}/blobs/uploads/{sessionId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response end4b(@RestPath("name") String name, @RestPath("sessionId") String sessionId, @RestQuery("digest") String digest, byte[] body) {
        try {
            LOG.info("Uploading for session: {}", sessionId);
            OCILayout ociLayout = OCILayout.Builder.builder().defaults(java.nio.file.Path.of(name)).build();
            LayoutRef layoutRef = LayoutRef.parse("%s@%s".formatted(ociLayout.getPath(), digest));
            Layer layer = ociLayout.pushBlob(layoutRef, body);
            LOG.info("Pushed blob: {}", layer.getDigest());
            return Response.created(URI.create("/v2/%s/blobs/%s".formatted(name, layer.getDigest()))).build();
        }
        catch (Exception e) {
            LOG.warn("Failed to create upload", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
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

        // Return location header if digest is null
        if (digest == null) {
            return Response.accepted().header(Const.LOCATION_HEADER, "/v2/%s/blobs/uploads/%s".formatted(name, UUID.randomUUID().toString())).build();
        }

        try {
            OCILayout ociLayout = OCILayout.Builder.builder().defaults(java.nio.file.Path.of(name)).build();
            LayoutRef layoutRef = LayoutRef.parse("%s@%s".formatted(ociLayout.getPath(), digest));
            Layer layer = ociLayout.pushBlob(layoutRef, body);
            LOG.info("Pushed blob: {}", layer.getDigest());
            return Response.created(URI.create("/v2/%s/blobs/%s".formatted(name, layer.getDigest()))).build();
        }
        catch (Exception e) {
            LOG.warn("Failed to create upload", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }

    @GET
    @Path("{name}/tags/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response end8a(@RestPath("name") String name) {
        try {
            if (!Files.isDirectory(java.nio.file.Path.of(name))) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            OCILayout ociLayout = OCILayout.Builder.builder().defaults(java.nio.file.Path.of(name)).build();
            LayoutRef layoutRef = LayoutRef.parse(name);
            return Response.ok(JsonUtils.toJson(ociLayout.getTags(layoutRef))).build();
        }
        catch (Exception e) {
            LOG.warn("Failed to get tags", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }

}
