package org.acme;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.mutiny.core.Vertx;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestMulti;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Path("/")
public class StreamResource {

    @Inject
    Vertx vertx;

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Multi<Buffer> stream(
        @Context HttpServerRequest request
    ) throws Exception {
        var filePath = "BigBuckBunny.mp4";

        String range = request.getHeader("Range");
        Long rangeFrom = getRangeFrom(range);

        RestMulti<Buffer> responseMulti = RestMulti.fromUniResponse(
            vertx.fileSystem().open(filePath, new OpenOptions().setRead(true)),
            (response) -> {
                response.setReadPos(rangeFrom);
                return response.toMulti().map(buffer -> {
                    return Buffer.buffer(buffer.getBytes());
                })
//                //delay
//                .onItem().call(buffer -> {
//                    return Uni.createFrom().nullItem().onItem().delayIt().by(Duration.ofMillis(20));
//                })
                    .onTermination().call((throwable, aBoolean) -> {
                        Log.info("before close on termination");
                        return response.close()
                            .onTermination().invoke(() -> {
                                Log.info("after close on termination");
                            })
                        ;
                    })
                ;
            },
            (response) -> {
                Long size = response.sizeBlocking();
                Map<String, List<String>> headers = new HashMap<>(Map.of(
                    "Accept-Ranges", List.of("bytes"),
                    "Content-Length", List.of(String.valueOf(size)),
                    "Content-Type", List.of("video/mp4")
                ));

                if (range != null) {
                    headers.put("Content-Range", List.of(String.format("bytes %s-%s/%s", rangeFrom, size - 1, size)));
                }

                return headers;
            },
            (response) -> {
                if (range != null) {
                    return Response.Status.PARTIAL_CONTENT.getStatusCode();
                }
                return Response.Status.OK.getStatusCode();
            }
        );

        return responseMulti;
    }

    private Long getRangeFrom(String range) {
        if (range == null) {
            return 0L;
        }

        Matcher matcher = Pattern.compile("^bytes=(\\d+)-(\\d+)?").matcher(range);

        if (matcher.matches() && matcher.groupCount() >= 1) {
            return Long.valueOf(matcher.group(1));
        }

        return 0L;
    }
}
