package io.github.jonesbusy;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import land.oras.ContainerRef;
import land.oras.LocalPath;
import land.oras.Registry;
import land.oras.utils.ArchiveUtils;
import land.oras.utils.Const;

@Path("/client")
public class Client {

    @Inject
    @Named("docker")
    Registry registry;

    @Inject
    @Named("localhost")
    Registry localhost;

    @Inject
    @Named("localhost2")
    Registry localhost2;

    @GET
    @Path("/compress")
    @Produces(MediaType.TEXT_PLAIN)
    public String compress() throws Exception {
        LocalPath tar = ArchiveUtils.tar(LocalPath.of("src"));
        ArchiveUtils.compress(tar, Const.DEFAULT_BLOB_DIR_MEDIA_TYPE); // gzip
        ArchiveUtils.compress(tar, Const.BLOB_DIR_ZSTD_MEDIA_TYPE); // zstd
        return "ok";
    }

    @GET
    @Path("/push")
    @Produces(MediaType.APPLICATION_JSON)
    public String push() throws Exception {
        return localhost
                .pushArtifact(ContainerRef.parse("test:latest"), LocalPath.of(java.nio.file.Path.of("pom.xml")))
                .getJson();
    }

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
