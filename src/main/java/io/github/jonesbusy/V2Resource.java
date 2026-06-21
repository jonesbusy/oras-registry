package io.github.jonesbusy;

import io.quarkiverse.oras.runtime.OCILayouts;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import land.oras.ArtifactType;
import land.oras.Index;
import land.oras.Layer;
import land.oras.LayoutRef;
import land.oras.Manifest;
import land.oras.OCILayout;
import land.oras.Referrers;
import land.oras.Repositories;
import land.oras.Tags;
import land.oras.exception.OrasException;
import land.oras.utils.Const;
import land.oras.utils.JsonUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestPath;
import org.jboss.resteasy.reactive.RestQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OCI Distribution Spec v1.1 — partial implementation for docker push/pull and core registry ops.
 */
@Path("/v2")
public class V2Resource {

    @Inject
    OCILayouts ociLayouts;

    @ConfigProperty(name = "quarkus.oras.layouts.path")
    String layoutsPath;

    private static final Logger LOG = LoggerFactory.getLogger(V2Resource.class);

    // In-flight blob data keyed by upload session ID (PATCH → PUT flow)
    private final ConcurrentHashMap<String, byte[]> uploadSessions = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // end-1: API version check
    // -------------------------------------------------------------------------

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response end1() {
        return Response.ok("OK")
                .header("Docker-Distribution-API-Version", "registry/2.0")
                .build();
    }

    // -------------------------------------------------------------------------
    // end-2: Blob pull
    // -------------------------------------------------------------------------

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
            LOG.debug("Blob not found: {}/{}", name, digest);
            return Response.status(404)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(OciError.of("BLOB_UNKNOWN", "blob unknown to registry"))
                    .build();
        }
    }

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
            return Response.status(404).build();
        }
    }

    // -------------------------------------------------------------------------
    // end-10: Delete blob
    // -------------------------------------------------------------------------

    @DELETE
    @Path("{name}/blobs/{digest}")
    public Response blobDelete(@RestPath("name") String name, @RestPath String digest) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            // Verify existence first
            ociLayout.getBlob(LayoutRef.of(ociLayout).withDigest(digest));
            String hex = digest.substring(digest.indexOf(':') + 1);
            Files.deleteIfExists(ociLayout.getPath().resolve("blobs/sha256/" + hex));
            return Response.accepted().build();
        } catch (OrasException e) {
            return Response.status(404)
                    .entity(OciError.of("BLOB_UNKNOWN", "blob unknown to registry"))
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to delete blob: {}/{}", name, digest, e);
            return Response.status(500)
                    .entity(OciError.of("INTERNAL_SERVER_ERROR", e.getMessage()))
                    .build();
        }
    }

    // -------------------------------------------------------------------------
    // end-4a/4b/5/6: Blob upload (POST → PATCH → PUT)
    // -------------------------------------------------------------------------

    @POST
    @Path("{name}/blobs/uploads")
    public Response blobUploadPost(@RestPath("name") String name, @RestQuery("digest") String digest, byte[] body) {
        // Monolithic POST: client sent body + digest together
        if (digest != null && body != null && body.length > 0) {
            try {
                OCILayout ociLayout = ociLayouts.getLayout(name);
                ensureLayout(ociLayout);
                Layer layer = ociLayout.pushBlob(LayoutRef.of(ociLayout).withDigest(digest), body);
                return Response.created(URI.create("/v2/%s/blobs/%s".formatted(name, layer.getDigest())))
                        .header("Docker-Content-Digest", layer.getDigest())
                        .build();
            } catch (Exception e) {
                LOG.warn("Monolithic blob push failed", e);
                return Response.status(500)
                        .entity(OciError.of("BLOB_UPLOAD_INVALID", e.getMessage()))
                        .build();
            }
        }
        // Normal: return session URL; Docker will PATCH data then PUT to finalize
        String sessionId = UUID.randomUUID().toString();
        return Response.accepted()
                .header(Const.LOCATION_HEADER, "/v2/%s/blobs/uploads/%s".formatted(name, sessionId))
                .header("Docker-Upload-UUID", sessionId)
                .header("Range", "0-0")
                .build();
    }

    /** end-5: Query upload session status */
    @GET
    @Path("{name}/blobs/uploads/{sessionId}")
    public Response blobUploadStatus(@RestPath("name") String name, @RestPath("sessionId") String sessionId) {
        byte[] data = uploadSessions.get(sessionId);
        if (data == null) {
            return Response.status(404)
                    .entity(OciError.of("BLOB_UPLOAD_UNKNOWN", "upload session not found"))
                    .build();
        }
        return Response.noContent()
                .header(Const.LOCATION_HEADER, "/v2/%s/blobs/uploads/%s".formatted(name, sessionId))
                .header("Range", data.length > 0 ? "0-" + (data.length - 1) : "0-0")
                .header("Docker-Upload-UUID", sessionId)
                .build();
    }

    /** end-4b (partial): Receive blob chunk */
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

    /** end-6: Finalize blob upload */
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
            ensureLayout(ociLayout);
            Layer layer = ociLayout.pushBlob(LayoutRef.of(ociLayout).withDigest(digest), data);
            uploadSessions.remove(sessionId);
            LOG.info("Pushed blob: {}/{}", name, layer.getDigest());
            return Response.created(URI.create("/v2/%s/blobs/%s".formatted(name, layer.getDigest())))
                    .header("Docker-Content-Digest", layer.getDigest())
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to finalize blob upload", e);
            return Response.status(500)
                    .entity(OciError.of("BLOB_UPLOAD_INVALID", e.getMessage()))
                    .build();
        }
    }

    /** end-14: Cancel blob upload */
    @DELETE
    @Path("{name}/blobs/uploads/{sessionId}")
    public Response blobUploadDelete(@RestPath("name") String name, @RestPath("sessionId") String sessionId) {
        uploadSessions.remove(sessionId);
        return Response.noContent().build();
    }

    // -------------------------------------------------------------------------
    // end-3: Manifest pull
    // -------------------------------------------------------------------------

    @HEAD
    @Path("{name}/manifests/{reference}")
    public Response manifestHead(@RestPath("name") String name, @RestPath("reference") String reference) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            String json = resolveManifestJson(ociLayout, reference);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            return Response.ok()
                    .header("Content-Type", resolveContentType(json))
                    .header(Const.CONTENT_LENGTH_HEADER, bytes.length)
                    .header("Docker-Content-Digest", sha256Hex(bytes))
                    .build();
        } catch (OrasException e) {
            return Response.status(404)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(OciError.of("MANIFEST_UNKNOWN", "manifest unknown"))
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to check manifest: {}/{}", name, reference, e);
            return Response.status(500)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(OciError.of("INTERNAL_SERVER_ERROR", e.getMessage()))
                    .build();
        }
    }

    @GET
    @Path("{name}/manifests/{reference}")
    @Produces(MediaType.WILDCARD)
    public Response manifestGet(@RestPath("name") String name, @RestPath("reference") String reference) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            String json = resolveManifestJson(ociLayout, reference);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            return Response.ok(json)
                    .header("Content-Type", resolveContentType(json))
                    .header("Docker-Content-Digest", sha256Hex(bytes))
                    .build();
        } catch (OrasException e) {
            return Response.status(404)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(OciError.of("MANIFEST_UNKNOWN", "manifest unknown"))
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to get manifest: {}/{}", name, reference, e);
            return Response.status(500)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(OciError.of("INTERNAL_SERVER_ERROR", e.getMessage()))
                    .build();
        }
    }

    // -------------------------------------------------------------------------
    // end-7: Push manifest or index
    // -------------------------------------------------------------------------

    @PUT
    @Path("{name}/manifests/{reference}")
    public Response manifestPut(
            @RestPath("name") String name,
            @RestPath("reference") String reference,
            @HeaderParam("Content-Type") String contentType,
            byte[] body) {
        try {
            String bodyJson = new String(body, StandardCharsets.UTF_8);
            OCILayout ociLayout = ociLayouts.getLayout(name);
            ensureLayout(ociLayout);
            LayoutRef ref = layoutRef(ociLayout, reference);
            if (isIndexType(contentType, bodyJson)) {
                ociLayout.pushIndex(ref, Index.fromJson(bodyJson));
                LOG.info("Pushed index: {}/{}", name, reference);
            } else {
                ociLayout.pushManifest(ref, Manifest.fromJson(bodyJson));
                LOG.info("Pushed manifest: {}/{}", name, reference);
            }
            // Use the original body bytes — digest must match what the client sent
            byte[] pushedBytes = bodyJson.getBytes(StandardCharsets.UTF_8);
            String digest = sha256Hex(pushedBytes);
            return Response.status(201)
                    .header("Docker-Content-Digest", digest)
                    .header("Location", "/v2/%s/manifests/%s".formatted(name, digest))
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to push manifest: {}/{}", name, reference, e);
            return Response.status(500)
                    .entity(OciError.of("MANIFEST_INVALID", e.getMessage()))
                    .build();
        }
    }

    // -------------------------------------------------------------------------
    // end-9: Delete manifest
    // -------------------------------------------------------------------------

    @DELETE
    @Path("{name}/manifests/{reference}")
    public Response manifestDelete(@RestPath("name") String name, @RestPath("reference") String reference) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            String manifestJson = resolveManifestJson(ociLayout, reference);
            String manifestDigest = sha256Hex(manifestJson.getBytes(StandardCharsets.UTF_8));

            java.nio.file.Path indexPath = ociLayout.getPath().resolve("index.json");
            Index index = Index.fromPath(indexPath);
            index.getManifests().stream()
                    .filter(d -> manifestDigest.equals(d.getDigest()))
                    .findFirst()
                    .ifPresent(d -> {
                        try {
                            Files.writeString(
                                    indexPath, index.withRemovedDescriptor(d).toJson(), StandardCharsets.UTF_8);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    });

            String hex = manifestDigest.substring(manifestDigest.indexOf(':') + 1);
            Files.deleteIfExists(ociLayout.getPath().resolve("blobs/sha256/" + hex));
            return Response.accepted().build();
        } catch (OrasException e) {
            return Response.status(404)
                    .entity(OciError.of("MANIFEST_UNKNOWN", "manifest unknown"))
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to delete manifest: {}/{}", name, reference, e);
            return Response.status(500)
                    .entity(OciError.of("INTERNAL_SERVER_ERROR", e.getMessage()))
                    .build();
        }
    }

    // -------------------------------------------------------------------------
    // end-8: List tags (with pagination)
    // -------------------------------------------------------------------------

    @GET
    @Path("{name}/tags/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response tagsList(@RestPath("name") String name, @RestQuery("n") Integer n, @RestQuery("last") String last) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            Tags tags = (n != null)
                    ? ociLayout.getTags(LayoutRef.of(ociLayout), n, last)
                    : ociLayout.getTags(LayoutRef.of(ociLayout));

            Response.ResponseBuilder rb = Response.ok(JsonUtils.toJson(tags));
            // Add RFC 5988 Link header when there is a next page
            if (tags.last() != null && !tags.last().isEmpty()) {
                String link = "</v2/%s/tags/list?n=%d&last=%s>; rel=\"next\"".formatted(name, n, tags.last());
                rb.header("Link", link);
            }
            return rb.build();
        } catch (Exception e) {
            LOG.warn("Failed to list tags: {}", name, e);
            return Response.status(404)
                    .entity(OciError.of("NAME_UNKNOWN", "repository name not known to registry"))
                    .build();
        }
    }

    // -------------------------------------------------------------------------
    // end-12: Referrers (OCI 1.1)
    // -------------------------------------------------------------------------

    @GET
    @Path("{name}/referrers/{digest}")
    @Produces("application/vnd.oci.image.index.v1+json")
    public Response referrers(
            @RestPath("name") String name,
            @RestPath("digest") String digest,
            @RestQuery("artifactType") String artifactType) {
        try {
            OCILayout ociLayout = ociLayouts.getLayout(name);
            ArtifactType type = artifactType != null ? ArtifactType.from(artifactType) : null;
            Referrers referrers = ociLayout.getReferrers(LayoutRef.of(ociLayout).withDigest(digest), type);
            Response.ResponseBuilder rb = Response.ok(referrers.toJson());
            if (artifactType != null) {
                rb.header("OCI-Filters-Applied", "artifactType");
            }
            return rb.build();
        } catch (Exception e) {
            // Spec requires empty index (not 404) when there are no referrers
            LOG.debug("No referrers for {}/{}: {}", name, digest, e.getMessage());
            String emptyIndex =
                    "{\"schemaVersion\":2,\"mediaType\":\"application/vnd.oci.image.index.v1+json\",\"manifests\":[]}";
            return Response.ok(emptyIndex).build();
        }
    }

    // -------------------------------------------------------------------------
    // Catalog (_catalog)
    // -------------------------------------------------------------------------

    @GET
    @Path("_catalog")
    @Produces(MediaType.APPLICATION_JSON)
    public Response catalog(@RestQuery("n") Integer n, @RestQuery("last") String last) {
        try {
            java.nio.file.Path base = java.nio.file.Path.of(layoutsPath);
            if (!Files.isDirectory(base)) {
                return Response.ok(JsonUtils.toJson(new Repositories(List.of())))
                        .build();
            }
            List<String> repos;
            try (var stream = Files.list(base)) {
                repos = stream.filter(Files::isDirectory)
                        .map(p -> p.getFileName().toString())
                        .filter(r -> !r.startsWith("."))
                        .sorted()
                        .collect(Collectors.toList());
            }
            // Cursor-based pagination
            if (last != null) {
                int idx = repos.indexOf(last);
                repos = idx >= 0 ? repos.subList(idx + 1, repos.size()) : repos;
            }
            String nextLast = null;
            if (n != null && n > 0 && repos.size() > n) {
                repos = repos.subList(0, n);
                nextLast = repos.get(repos.size() - 1);
            }
            Response.ResponseBuilder rb = Response.ok(JsonUtils.toJson(new Repositories(repos)));
            if (nextLast != null) {
                rb.header("Link", "</v2/_catalog?n=%d&last=%s>; rel=\"next\"".formatted(n, nextLast));
            }
            return rb.build();
        } catch (Exception e) {
            LOG.warn("Failed to list catalog", e);
            return Response.status(500)
                    .entity(OciError.of("INTERNAL_SERVER_ERROR", e.getMessage()))
                    .build();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the raw JSON for a manifest or index stored at the given reference (tag or digest).
     * Reads the blob file directly instead of relying on SDK getJson() methods, which return null
     * when objects are constructed from disk rather than via fromJson().
     */
    private String resolveManifestJson(OCILayout ociLayout, String reference) {
        try {
            String digest;
            if (reference.startsWith("sha256:")) {
                digest = reference;
            } else {
                java.nio.file.Path indexPath = ociLayout.getPath().resolve("index.json");
                if (!Files.exists(indexPath)) {
                    throw new OrasException("manifest unknown: " + reference);
                }
                Index topIndex = Index.fromPath(indexPath);
                String resolved = topIndex.getManifests().stream()
                        .filter(d -> d.getAnnotations() != null
                                && reference.equals(d.getAnnotations().get("org.opencontainers.image.ref.name")))
                        .map(d -> d.getDigest())
                        .findFirst()
                        .orElse(null);
                if (resolved == null) {
                    throw new OrasException("manifest unknown: " + reference);
                }
                digest = resolved;
            }
            String hex = digest.substring(digest.indexOf(':') + 1);
            java.nio.file.Path blobPath = ociLayout.getPath().resolve("blobs/sha256/" + hex);
            if (!Files.exists(blobPath)) {
                throw new OrasException("manifest not found: " + digest);
            }
            return Files.readString(blobPath, StandardCharsets.UTF_8);
        } catch (OrasException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read manifest", e);
        }
    }

    /**
     * Ensures the full OCI layout skeleton exists before any push.
     * OCILayout$Builder.defaults() only sets the path field and creates the directory;
     * it does NOT write oci-layout or index.json. pushManifest/pushIndex call
     * Index.fromPath("index.json") and fail with NoSuchFileException on a fresh layout.
     */
    private void ensureLayout(OCILayout ociLayout) {
        try {
            java.nio.file.Path root = ociLayout.getPath();
            Files.createDirectories(root.resolve("blobs/sha256"));
            java.nio.file.Path ociLayoutFile = root.resolve("oci-layout");
            if (!Files.exists(ociLayoutFile)) {
                Files.writeString(ociLayoutFile, "{\"imageLayoutVersion\":\"1.0.0\"}", StandardCharsets.UTF_8);
            }
            java.nio.file.Path indexFile = root.resolve("index.json");
            if (!Files.exists(indexFile)) {
                Files.writeString(indexFile, "{\"schemaVersion\":2,\"manifests\":[]}", StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot initialize OCI layout", e);
        }
    }

    /** True when the Content-Type or JSON mediaType field indicates an OCI/Docker image index. */
    private boolean isIndexType(String contentType, String json) {
        return isIndexMediaType(contentType) || isIndexMediaType(extractMediaType(json));
    }

    private boolean isIndexMediaType(String mediaType) {
        if (mediaType == null) return false;
        return mediaType.contains("image.index") || mediaType.contains("manifest.list");
    }

    /**
     * Returns the correct Content-Type for a manifest or index JSON blob.
     * Uses structural detection because the ORAS SDK serializes "manifests" before the top-level
     * "mediaType" field, causing naive first-occurrence extraction to pick up a nested manifest type.
     * OCI indexes have "manifests" but no top-level "config"; manifests have "config".
     */
    private String resolveContentType(String json) {
        if (json.contains("\"manifests\"") && !json.contains("\"config\"")) {
            return "application/vnd.oci.image.index.v1+json";
        }
        String mediaType = extractMediaType(json);
        return mediaType.isEmpty() ? "application/vnd.oci.image.manifest.v1+json" : mediaType;
    }

    /**
     * Extracts the mediaType field value from a manifest/index JSON string without a full parse.
     * Returns an empty string if not found. NOTE: may match a nested occurrence when the top-level
     * field appears after array elements — use resolveContentType() for response headers.
     */
    private String extractMediaType(String json) {
        int idx = json.indexOf("\"mediaType\"");
        if (idx < 0) return "";
        int colon = json.indexOf(':', idx);
        if (colon < 0) return "";
        int start = json.indexOf('"', colon) + 1;
        int end = json.indexOf('"', start);
        return (start > 0 && end > start) ? json.substring(start, end).replace("\\/", "/") : "";
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
