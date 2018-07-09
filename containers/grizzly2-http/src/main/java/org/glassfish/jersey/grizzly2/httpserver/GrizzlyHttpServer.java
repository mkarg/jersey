package org.glassfish.jersey.grizzly2.httpserver;

import static javax.ws.rs.JAXRS.Configuration.SSLClientAuthentication.MANDATORY;
import static javax.ws.rs.JAXRS.Configuration.SSLClientAuthentication.OPTIONAL;

import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.net.ssl.SSLContext;
import javax.ws.rs.JAXRS;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.jersey.server.spi.Server;

public final class GrizzlyHttpServer implements Server {

    private final GrizzlyHttpContainer container;

    private final HttpServer httpServer;

    GrizzlyHttpServer(final Application application, final JAXRS.Configuration configuration)
            throws NoSuchAlgorithmException, KeyManagementException {
        final String protocol = configuration.protocol();
        final String host = configuration.host();
        final int port = configuration.port();
        final String rootPath = configuration.rootPath();
        final SSLContext sslContext = configuration.sslContext();
        final JAXRS.Configuration.SSLClientAuthentication sslClientAuthentication = configuration
                .sslClientAuthentication();
        final URI uri = UriBuilder.fromUri(protocol.toLowerCase() + "://" + host).port(port).path(rootPath).build();

        this.container = new GrizzlyHttpContainer(application);
        this.httpServer = GrizzlyHttpServerFactory.createHttpServer(uri, this.container, "HTTPS".equals(protocol),
                new SSLEngineConfigurator(sslContext, false, sslClientAuthentication == OPTIONAL,
                        sslClientAuthentication == MANDATORY),
                true);
    }

    @Override
    public final GrizzlyHttpContainer container() {
        return this.container;
    }

    @Override
    public final int port() {
        return this.httpServer.getListener("grizzly").getPort();
    }

    @Override
    public final CompletionStage<?> stop() {
        return CompletableFuture.runAsync(this.httpServer::shutdownNow);
    }

    @Override
    public final <T> T unwrap(final Class<T> nativeClass) {
        return nativeClass.cast(this.httpServer);
    }

}
