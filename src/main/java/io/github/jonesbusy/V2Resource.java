package io.github.jonesbusy;

import io.quarkiverse.oras.runtime.OCILayouts;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;
import land.oras.Layer;
import land.oras.LayoutRef;
import land.oras.Manifest;
import land.oras.OCILayout;
import land.oras.exception.OrasException;
import land.oras.utils.Const;
import land.oras.utils.JsonUtils;
import org.jboss.resteasy.reactive.RestPath;
import org.jboss.resteasy.reactive.RestQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OCI partial spec
 */
@Path("/v2")
public class V2Resource {

    @Inject
    OCILayouts ociLayouts;

    /**
     * Logger
     */
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
            OCILayout ociLayout = ociLayouts.getLayout(name);
            return Response.ok(ociLayout.getBlob(LayoutRef.of(ociLayout).withDigest(digest)))
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to get blob", e);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(e.getMessage())
                    .build();
        }
    }

    /**
     * end-2
     */
    @HEAD
    @Path("{name}/blobs/{digest}")
    public Response end2Head(@RestPath("name") String name, @RestPath String digest) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            byte[] blob = ociLayout.getBlob(LayoutRef.of(ociLayout).withDigest(digest));
            return Response.ok()
                    .header(Const.CONTENT_LENGTH_HEADER, blob.length)
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to get blob", e);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(e.getMessage())
                    .build();
        }
    }

    /**
     * Monolithic upload
     */
    @PUT
    @Path("{name}/blobs/uploads/{sessionId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response end4b(
            @RestPath("name") String name,
            @RestPath("sessionId") String sessionId,
            @RestQuery("digest") String digest,
            byte[] body) {
        try {
            LOG.info("Uploading for session: {}", sessionId);
            OCILayout ociLayout = ociLayouts.getLayout(name);
            Layer layer = ociLayout.pushBlob(LayoutRef.of(ociLayout).withDigest(digest), body);
            LOG.info("Pushed blob: {}", layer.getDigest());
            return Response.created(URI.create("/v2/%s/blobs/%s".formatted(name, layer.getDigest())))
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to create upload", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
        }
    }

    @HEAD
    @Path("{name}/manifests/{reference}")
    public Response headend3(@RestPath("name") String name, @RestPath("reference") String digest) {
        try {
            LOG.info("Checking manifest with ref: {}", digest);
            OCILayout ociLayout = ociLayouts.getLayout(name);
            ociLayout.getManifest(LayoutRef.of(ociLayout).withDigest(digest));
            return Response.status(200).build();
        }
        // Not found
        catch (OrasException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } catch (Exception e) {
            LOG.warn("Failed to check manifest", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
        }
    }

    @GET
    @Path("{name}/manifests/{reference}")
    @Produces(Const.DEFAULT_MANIFEST_MEDIA_TYPE)
    public Response getend3(@RestPath("name") String name, @RestPath("reference") String digest) {
        try {
            LOG.info("Getting manifest with ref: {}", digest);
            OCILayout ociLayout = ociLayouts.getLayout(name);
            Manifest manifest = ociLayout.getManifest(LayoutRef.of(ociLayout).withDigest(digest));
            LOG.info("Found manifest: {}", manifest.getJson());
            return Response.ok(manifest.getJson()).build();
        }
        // Not found
        catch (OrasException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } catch (Exception e) {
            LOG.warn("Failed to upload manifet", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
        }
    }

    @PUT
    @Path("{name}/manifests/{reference}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response end7(@RestPath("name") String name, @RestPath("reference") String digest, byte[] body) {
        try {
            Manifest manifest = Manifest.fromJson(new String(body, StandardCharsets.UTF_8));
            LOG.info("Uploading manifest: {}", manifest.getJson());
            OCILayout ociLayout = ociLayouts.getLayout(name);
            ociLayout.pushManifest(LayoutRef.of(ociLayout).withDigest(digest), manifest);
            LOG.info("Pushed manifest: {}", manifest.getJson());
            return Response.status(201).build();
        } catch (Exception e) {
            LOG.warn("Failed to upload manifet", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
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
            return Response.accepted()
                    .header(
                            Const.LOCATION_HEADER,
                            "/v2/%s/blobs/uploads/%s"
                                    .formatted(name, UUID.randomUUID().toString()))
                    .build();
        }

        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            Layer layer = ociLayout.pushBlob(LayoutRef.of(ociLayout).withDigest(digest), body);
            LOG.info("Pushed blob: {}", layer.getDigest());
            return Response.created(URI.create("/v2/%s/blobs/%s".formatted(name, layer.getDigest())))
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to create upload", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
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
            OCILayout ociLayout = ociLayouts.getLayout(name);
            return Response.ok(JsonUtils.toJson(ociLayout.getTags(LayoutRef.of(ociLayout))))
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to get tags", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
        }
    }
}
