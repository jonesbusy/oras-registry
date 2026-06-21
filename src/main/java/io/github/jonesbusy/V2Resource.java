package io.github.jonesbusy;

import io.quarkiverse.oras.runtime.OCILayouts;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
 * Partial OCI Distribution Spec v2 implementation.
 * Supports docker push/pull with monolithic and chunked (PATCH→PUT) blob uploads.
 */
@Path("/v2")
public class V2Resource {

    @Inject
    OCILayouts ociLayouts;

    private static final Logger LOG = LoggerFactory.getLogger(V2Resource.class);

    // In-flight blob data keyed by upload session ID (PATCH → PUT flow)
    private final ConcurrentHashMap<String, byte[]> uploadSessions = new ConcurrentHashMap<>();

    /** end-1: API version check */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response end1() {
        return Response.ok("OK")
                .header("Docker-Distribution-API-Version", "registry/2.0")
                .build();
    }

    /** end-2: Pull blob */
    @GET
    @Path("{name}/blobs/{digest}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response blobGet(@RestPath("name") String name, @RestPath String digest) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            byte[] blob = ociLayout.getBlob(LayoutRef.of(ociLayout).withDigest(digest));
            return Response.ok(blob)
                    .header("Docker-Content-Digest", digest)
                    .header(Const.CONTENT_LENGTH_HEADER, blob.length)
                    .build();
        } catch (Exception e) {
            LOG.warn("Blob not found: {}/{}", name, digest);
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    /** end-2: Check blob exists */
    @HEAD
    @Path("{name}/blobs/{digest}")
    public Response blobHead(@RestPath("name") String name, @RestPath String digest) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            byte[] blob = ociLayout.getBlob(LayoutRef.of(ociLayout).withDigest(digest));
            return Response.ok()
                    .header(Const.CONTENT_LENGTH_HEADER, blob.length)
                    .header("Docker-Content-Digest", digest)
                    .build();
        } catch (Exception e) {
            LOG.debug("Blob not found (HEAD): {}/{}", name, digest);
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    /** end-4a: Initiate blob upload session */
    @POST
    @Path("{name}/blobs/uploads")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response blobUploadPost(@RestPath("name") String name, @RestQuery("digest") String digest, byte[] body) {
        // Monolithic POST: client sent body + digest in one shot
        if (digest != null && body != null && body.length > 0) {
            try {
                OCILayout ociLayout = ociLayouts.getLayout(name);
                Layer layer = ociLayout.pushBlob(LayoutRef.of(ociLayout).withDigest(digest), body);
                return Response.created(URI.create("/v2/%s/blobs/%s".formatted(name, layer.getDigest())))
                        .header("Docker-Content-Digest", layer.getDigest())
                        .build();
            } catch (Exception e) {
                LOG.warn("Failed monolithic blob push", e);
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(e.getMessage())
                        .build();
            }
        }
        // Normal flow: return session URL; client will PATCH data then PUT to finalize
        String sessionId = UUID.randomUUID().toString();
        return Response.accepted()
                .header(Const.LOCATION_HEADER, "/v2/%s/blobs/uploads/%s".formatted(name, sessionId))
                .header("Docker-Upload-UUID", sessionId)
                .header("Range", "0-0")
                .build();
    }

    /** end-4b (partial): Receive blob chunk — stores data for finalization via PUT */
    @PATCH
    @Path("{name}/blobs/uploads/{sessionId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response blobUploadPatch(
            @RestPath("name") String name, @RestPath("sessionId") String sessionId, byte[] body) {
        byte[] data = body != null ? body : new byte[0];
        uploadSessions.put(sessionId, data);
        String location = "/v2/%s/blobs/uploads/%s".formatted(name, sessionId);
        return Response.accepted()
                .header(Const.LOCATION_HEADER, location)
                .header("Range", data.length > 0 ? "0-" + (data.length - 1) : "0-0")
                .header("Docker-Upload-UUID", sessionId)
                .build();
    }

    /** end-4b: Finalize blob upload — uses session data from PATCH when body is absent */
    @PUT
    @Path("{name}/blobs/uploads/{sessionId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response blobUploadPut(
            @RestPath("name") String name,
            @RestPath("sessionId") String sessionId,
            @RestQuery("digest") String digest,
            byte[] body) {
        try {
            byte[] data = (body != null && body.length > 0) ? body : uploadSessions.remove(sessionId);
            if (data == null) {
                data = new byte[0];
            }
            OCILayout ociLayout = ociLayouts.getLayout(name);
            Layer layer = ociLayout.pushBlob(LayoutRef.of(ociLayout).withDigest(digest), data);
            uploadSessions.remove(sessionId);
            LOG.info("Pushed blob: {}/{}", name, layer.getDigest());
            return Response.created(URI.create("/v2/%s/blobs/%s".formatted(name, layer.getDigest())))
                    .header("Docker-Content-Digest", layer.getDigest())
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to finalize blob upload", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
        }
    }

    /** end-3: Check manifest exists */
    @HEAD
    @Path("{name}/manifests/{reference}")
    public Response manifestHead(@RestPath("name") String name, @RestPath("reference") String reference) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            Manifest manifest = ociLayout.getManifest(layoutRef(ociLayout, reference));
            byte[] bytes = manifest.getJson().getBytes(StandardCharsets.UTF_8);
            return Response.ok()
                    .header("Content-Type", manifest.getMediaType())
                    .header(Const.CONTENT_LENGTH_HEADER, bytes.length)
                    .header("Docker-Content-Digest", sha256Hex(bytes))
                    .build();
        } catch (OrasException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } catch (Exception e) {
            LOG.warn("Failed to check manifest: {}/{}", name, reference, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
        }
    }

    /** end-3: Pull manifest */
    @GET
    @Path("{name}/manifests/{reference}")
    @Produces(MediaType.WILDCARD)
    public Response manifestGet(@RestPath("name") String name, @RestPath("reference") String reference) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            Manifest manifest = ociLayout.getManifest(layoutRef(ociLayout, reference));
            byte[] bytes = manifest.getJson().getBytes(StandardCharsets.UTF_8);
            return Response.ok(manifest.getJson())
                    .header("Content-Type", manifest.getMediaType())
                    .header("Docker-Content-Digest", sha256Hex(bytes))
                    .build();
        } catch (OrasException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } catch (Exception e) {
            LOG.warn("Failed to get manifest: {}/{}", name, reference, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
        }
    }

    /** end-7: Push manifest */
    @PUT
    @Path("{name}/manifests/{reference}")
    public Response manifestPut(@RestPath("name") String name, @RestPath("reference") String reference, byte[] body) {
        try {
            Manifest manifest = Manifest.fromJson(new String(body, StandardCharsets.UTF_8));
            OCILayout ociLayout = ociLayouts.getLayout(name);
            Manifest pushed = ociLayout.pushManifest(layoutRef(ociLayout, reference), manifest);
            byte[] bytes = pushed.getJson().getBytes(StandardCharsets.UTF_8);
            String digest = sha256Hex(bytes);
            LOG.info("Pushed manifest: {}/{} digest={}", name, reference, digest);
            return Response.status(201)
                    .header("Docker-Content-Digest", digest)
                    .header("Location", "/v2/%s/manifests/%s".formatted(name, digest))
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to push manifest: {}/{}", name, reference, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
        }
    }

    /** end-8a: List tags */
    @GET
    @Path("{name}/tags/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response tagsList(@RestPath("name") String name) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            return Response.ok(JsonUtils.toJson(ociLayout.getTags(LayoutRef.of(ociLayout))))
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to list tags: {}", name, e);
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    private LayoutRef layoutRef(OCILayout ociLayout, String reference) {
        return reference.startsWith("sha256:")
                ? LayoutRef.of(ociLayout).withDigest(reference)
                : LayoutRef.of(ociLayout).withTag(reference);
    }

    private String sha256Hex(byte[] data) {
        try {
            return "sha256:"
                    + HexFormat.of()
                            .formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
