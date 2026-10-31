/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v 2.0, which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the
 * Eclipse Public License v 2.0 are satisfied: GNU General Public License,
 * version 2 with the GNU Classpath Exception, which is available at
 * https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 */

package org.glassfish.jersey.netty.connector;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests the HTTP/2 (the cleartext HTTP/2 prior knowledge, i.e. h2c) support of the Netty connector.
 */
public class Http2Test {

    private static final String GET_RESPONSE = "Hello World!";
    private static final String POST_RESPONSE = "Posted!";
    private static final String SLOW_RESPONSE = "0123456789";

    private Channel serverChannel;
    private NioEventLoopGroup serverGroup;
    private int port;
    private final AtomicInteger acceptedConnections = new AtomicInteger();

    @BeforeEach
    public void startServer() throws Exception {
        serverGroup = new NioEventLoopGroup(1);
        ServerBootstrap b = new ServerBootstrap();
        b.group(serverGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 acceptedConnections.incrementAndGet();
                 ch.pipeline().addLast(
                         Http2FrameCodecBuilder.forServer().build(),
                         new Http2MultiplexHandler(new Http2ServerStreamHandler()));
             }
         });
        serverChannel = b.bind(0).sync().channel();
        port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @AfterEach
    public void stopServer() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        serverGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
    }

    /** Remembers the request headers of the HTTP/2 stream (the stream handler is shared). */
    private static final io.netty.util.AttributeKey<Http2Headers> STREAM_REQUEST_HEADERS =
            io.netty.util.AttributeKey.valueOf("streamRequestHeaders");
    /** Collects the request payload of the HTTP/2 stream until END_STREAM. */
    private static final io.netty.util.AttributeKey<StringBuilder> STREAM_REQUEST_BODY =
            io.netty.util.AttributeKey.valueOf("streamRequestBody");

    /**
     * A minimal HTTP/2 server stream handler: echoes the request method and the collected request payload
     * in the response.
     */
    @io.netty.channel.ChannelHandler.Sharable
    private static final class Http2ServerStreamHandler extends SimpleChannelInboundHandler<Object> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final Object msg) {
            if (msg instanceof Http2HeadersFrame) {
                final Http2HeadersFrame headersFrame = (Http2HeadersFrame) msg;
                ctx.channel().attr(STREAM_REQUEST_HEADERS).set(headersFrame.headers());
                if (headersFrame.isEndStream()) {
                    // a request without an entity: respond immediately
                    respond(ctx, "");
                }
                return;
            }
            if (msg instanceof io.netty.handler.codec.http2.Http2DataFrame) {
                // collect the request payload across the DATA frames until END_STREAM
                final io.netty.handler.codec.http2.Http2DataFrame dataFrame =
                        (io.netty.handler.codec.http2.Http2DataFrame) msg;
                StringBuilder body = ctx.channel().attr(STREAM_REQUEST_BODY).get();
                if (body == null) {
                    body = new StringBuilder();
                    ctx.channel().attr(STREAM_REQUEST_BODY).set(body);
                }
                body.append(dataFrame.content().toString(StandardCharsets.UTF_8));
                if (dataFrame.isEndStream()) {
                    respond(ctx, body.toString());
                }
            }
        }

        private void respond(final ChannelHandlerContext ctx, final String requestBody) {
            final Http2Headers headers = ctx.channel().attr(STREAM_REQUEST_HEADERS).get();
            final boolean isPost = "POST".contentEquals(headers.method());
            final boolean isSlow = String.valueOf(headers.path()).contains("/slow");
            final boolean isInformational = String.valueOf(headers.path()).contains("/informational");
            final boolean isSmall = String.valueOf(headers.path()).contains("/small");

            if (isInformational) {
                // the final response is preceded by an informational (103 Early Hints) response
                final Http2Headers earlyHints = new DefaultHttp2Headers()
                        .status("103")
                        .set("x-early-hints", "yes");
                ctx.write(new DefaultHttp2HeadersFrame(earlyHints, false));
                ctx.flush();
            }

            final Http2Headers response = new DefaultHttp2Headers()
                    .status("200")
                    .set(HttpHeaderNames.CONTENT_TYPE, MediaType.TEXT_PLAIN)
                    .set("x-http2-echo-method", String.valueOf(headers.method()));

            ctx.write(new DefaultHttp2HeadersFrame(response, false));
            if (isSlow) {
                // deliver the response body byte-by-byte, each byte in its own DATA frame, with the gaps
                // shorter than the client's read-timeout interval, but the total delivery time spanning
                // several read-timeout intervals (which the old pipeline layout would wrongly trigger on)
                final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
                final AtomicInteger remaining = new AtomicInteger(SLOW_RESPONSE.length());
                for (int i = 0; i < SLOW_RESPONSE.length(); i++) {
                    final int index = i;
                    scheduler.schedule(() -> {
                        final ByteBuf oneByte = Unpooled.wrappedBuffer(
                                new byte[] {(byte) SLOW_RESPONSE.charAt(index)});
                        ctx.write(new DefaultHttp2DataFrame(oneByte,
                                remaining.decrementAndGet() == 0));
                        ctx.flush();
                    }, 500L * (i + 1), TimeUnit.MILLISECONDS);
                }
                scheduler.shutdown();
            } else {
                // echo the collected request payload (verifies the entity writing over the HTTP/2
                // pipeline), or answer with the fixed body otherwise
                final String body = isPost ? "echo:" + requestBody
                        : isSmall ? "ok" : GET_RESPONSE;
                final ByteBuf content = Unpooled.wrappedBuffer(body.getBytes(StandardCharsets.UTF_8));
                ctx.write(new DefaultHttp2DataFrame(content, true));
            }
            ctx.flush();
        }
    }

    private Client http2Client() {
        final ClientConfig config = new ClientConfig();
        config.connectorProvider(new NettyConnectorProvider());
        config.property(NettyClientProperties.ENABLE_HTTP2, Boolean.TRUE);
        return ClientBuilder.newClient(config);
    }

    @Test
    public void testGetHttp2() {
        try (Client client = http2Client()) {
            try (Response response = client.target("http://localhost:" + port + "/helloworld")
                    .request(MediaType.TEXT_PLAIN_TYPE).get()) {
                assertEquals(200, response.getStatus());
                assertEquals(GET_RESPONSE, response.readEntity(String.class));
                assertEquals("GET", response.getHeaderString("x-http2-echo-method"));
            }
        }
    }

    @Test
    public void testPostHttp2() {
        try (Client client = http2Client()) {
            try (Response response = client.target("http://localhost:" + port + "/helloworld")
                    .request(MediaType.TEXT_PLAIN_TYPE).post(Entity.entity("payload", MediaType.TEXT_PLAIN_TYPE))) {
                assertEquals(200, response.getStatus());
                // the server collects the request payload until END_STREAM and echoes it back:
                // this verifies the entity writing over the HTTP/2 pipeline
                assertEquals("echo:payload", response.readEntity(String.class));
                assertEquals("POST", response.getHeaderString("x-http2-echo-method"));
            }
        }
    }

    @Test
    public void testConnectionReuseHttp2() {
        try (Client client = http2Client()) {
            for (int i = 0; i < 3; i++) {
                try (Response response = client.target("http://localhost:" + port + "/helloworld")
                        .request(MediaType.TEXT_PLAIN_TYPE).get()) {
                    assertEquals(200, response.getStatus());
                    assertEquals(GET_RESPONSE, response.readEntity(String.class));
                }
            }
        }
        // all three requests must be served over the same pooled connection
        assertEquals(1, acceptedConnections.get());
    }

    @Test
    public void testHttp1RequestFailsAgainstHttp2OnlyServer() {
        final ClientConfig config = new ClientConfig();
        config.connectorProvider(new NettyConnectorProvider());
        try (Client client = ClientBuilder.newClient(config)) {
            // the HTTP/2-only server rejects the plain HTTP/1.1 request; the invocation must not succeed
            assertThrows(Exception.class, () -> {
                client.target("http://localhost:" + port + "/helloworld").request().get(String.class);
            });
        }
    }

    /**
     * An informational response (e.g. 100 Continue or 103 Early Hints) preceding the final response
     * must not complete the request: the caller has to receive the final response only.
     */
    @Test
    public void testInformationalResponsePrecedesFinalResponse() {
        try (Client client = http2Client()) {
            try (Response response = client.target("http://localhost:" + port + "/informational")
                    .request(MediaType.TEXT_PLAIN_TYPE).get()) {
                assertEquals(200, response.getStatus());
                assertEquals(GET_RESPONSE, response.readEntity(String.class));
            }
        }
    }

    /**
     * Exceeding MAX_HTTP2_CONTENT_LENGTH on a reused connection must fail the current request: the
     * connection-scoped error forwarding must reach the request handler installed for this request,
     * not the already completed completion of the first request.
     */
    @Test
    public void testContentLimitExceededOnReusedConnectionFailsRequest() {
        final ClientConfig config = new ClientConfig();
        config.connectorProvider(new NettyConnectorProvider());
        config.property(NettyClientProperties.ENABLE_HTTP2, Boolean.TRUE);
        config.property(NettyClientProperties.MAX_HTTP2_CONTENT_LENGTH, 8);
        try (Client client = ClientBuilder.newClient(config)) {
            // the first request (a body below the limit) succeeds and returns the connection to the pool
            try (Response response = client.target("http://localhost:" + port + "/small")
                    .request(MediaType.TEXT_PLAIN_TYPE).get()) {
                assertEquals(200, response.getStatus());
                assertEquals("ok", response.readEntity(String.class));
            }
            // the second request over the reused connection exceeds the content limit and must fail
            assertThrows(ProcessingException.class, () -> {
                client.target("http://localhost:" + port + "/helloworld")
                        .request(MediaType.TEXT_PLAIN_TYPE).get(String.class);
            });
        }
    }

    /**
     * An informational response preceding the final response over the plain HTTP/1.1 must not complete
     * the request either: the content terminator of the 1xx response must be ignored too.
     */
    @Test
    public void testHttp1InformationalResponsePrecedesFinalResponse() throws InterruptedException {
        // a plain HTTP/1.1 server that sends a 103 Early Hints response before the final 200
        final NioEventLoopGroup http1Group = new NioEventLoopGroup(1);
        Channel http1Server = null;
        try {
            final ServerBootstrap http1 = new ServerBootstrap();
            http1.group(http1Group)
                 .channel(NioServerSocketChannel.class)
                 .childHandler(new ChannelInitializer<SocketChannel>() {
                     @Override
                     protected void initChannel(SocketChannel ch) {
                         ch.pipeline().addLast(new io.netty.handler.codec.http.HttpServerCodec(),
                                 new io.netty.handler.codec.http.HttpObjectAggregator(1 << 16),
                                 new SimpleChannelInboundHandler<io.netty.handler.codec.http.FullHttpRequest>() {
                                     @Override
                                     protected void channelRead0(final ChannelHandlerContext ctx,
                                             final io.netty.handler.codec.http.FullHttpRequest request) {
                                         // the informational response with its own empty terminator
                                         final io.netty.handler.codec.http.DefaultHttpResponse earlyHints =
                                                 new io.netty.handler.codec.http.DefaultHttpResponse(
                                                         io.netty.handler.codec.http.HttpVersion.HTTP_1_1,
                                                         new io.netty.handler.codec.http.HttpResponseStatus(103,
                                                                 "Early Hints"));
                                         ctx.write(earlyHints);
                                         ctx.writeAndFlush(
                                                 io.netty.handler.codec.http.LastHttpContent.EMPTY_LAST_CONTENT);
                                         // the final response
                                         final io.netty.handler.codec.http.DefaultFullHttpResponse finalResponse =
                                                 new io.netty.handler.codec.http.DefaultFullHttpResponse(
                                                         io.netty.handler.codec.http.HttpVersion.HTTP_1_1,
                                                         io.netty.handler.codec.http.HttpResponseStatus.OK,
                                                         io.netty.buffer.Unpooled.wrappedBuffer(
                                                                 GET_RESPONSE.getBytes(StandardCharsets.UTF_8)));
                                         finalResponse.headers().set(HttpHeaderNames.CONTENT_TYPE,
                                                 MediaType.TEXT_PLAIN);
                                         ctx.writeAndFlush(finalResponse)
                                                 .addListener(io.netty.channel.ChannelFutureListener.CLOSE);
                                     }
                                 });
                     }
                 });
            http1Server = http1.bind(0).sync().channel();
            final int http1Port = ((InetSocketAddress) http1Server.localAddress()).getPort();

            final ClientConfig config = new ClientConfig();
            config.connectorProvider(new NettyConnectorProvider());
            try (Client client = ClientBuilder.newClient(config)) {
                try (Response response = client.target("http://localhost:" + http1Port + "/helloworld")
                        .request(MediaType.TEXT_PLAIN_TYPE).get()) {
                    assertEquals(200, response.getStatus());
                    assertEquals(GET_RESPONSE, response.readEntity(String.class));
                }
            }
        } finally {
            if (http1Server != null) {
                http1Server.close();
            }
            http1Group.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
    }

    /**
     * Exceeding MAX_HTTP2_CONTENT_LENGTH must fail the request instead of leaving it waiting indefinitely.
     */
    @Test
    public void testContentLimitExceededFailsRequest() {
        final ClientConfig config = new ClientConfig();
        config.connectorProvider(new NettyConnectorProvider());
        config.property(NettyClientProperties.ENABLE_HTTP2, Boolean.TRUE);
        config.property(NettyClientProperties.MAX_HTTP2_CONTENT_LENGTH, 8);
        try (Client client = ClientBuilder.newClient(config)) {
            assertThrows(ProcessingException.class, () -> {
                client.target("http://localhost:" + port + "/helloworld")
                        .request(MediaType.TEXT_PLAIN_TYPE).get(String.class);
            });
        }
    }

    /**
     * A peer that accepts the TCP connection but never completes the TLS handshake must fail the request
     * once the read timeout elapses, instead of waiting for the connect timeout (which is unbounded when unset).
     */
    @Test
    public void testReadTimeoutDuringHttpsHandshake() throws InterruptedException {
        // a server that accepts connections but never speaks TLS
        final NioEventLoopGroup silentGroup = new NioEventLoopGroup(1);
        Channel silentServer = null;
        try {
            final ServerBootstrap silent = new ServerBootstrap();
            silent.group(silentGroup)
                  .channel(NioServerSocketChannel.class)
                  .childHandler(new ChannelInitializer<SocketChannel>() {
                      @Override
                      protected void initChannel(SocketChannel ch) {
                          // no handler: the server never replies to the TLS handshake
                      }
                  });
            silentServer = silent.bind(0).sync().channel();
            final int silentPort = ((InetSocketAddress) silentServer.localAddress()).getPort();

            final ClientConfig config = new ClientConfig();
            config.connectorProvider(new NettyConnectorProvider());
            config.property(NettyClientProperties.ENABLE_HTTP2, Boolean.TRUE);
            config.property(ClientProperties.READ_TIMEOUT, 1_500);
            try (Client client = ClientBuilder.newClient(config)) {
                final long start = System.nanoTime();
                assertThrows(ProcessingException.class, () -> {
                    client.target("https://localhost:" + silentPort + "/helloworld")
                            .request(MediaType.TEXT_PLAIN_TYPE).get(String.class);
                });
                final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                // the request must fail because of the read timeout (plus a scheduling margin), and not
                // because of the separate SSL handshake timeout, which would take much longer
                assertTrue(elapsedMillis < 5_000,
                        "The request should fail shortly after the read timeout, but took "
                                + elapsedMillis + " ms");
            }
        } finally {
            if (silentServer != null) {
                silentServer.close();
            }
            silentGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
    }

    /**
     * The read timeout is an inactivity timeout: an HTTP/2 response whose DATA frames keep arriving over
     * more than one read-timeout interval (but with no gap ever exceeding the timeout) must not time out.
     */
    @Test
    public void testReadTimeoutNotTriggeredByProgressingHttp2Response() {
        try (Client client = http2Client()) {
            client.property(ClientProperties.READ_TIMEOUT, 2_000);
            try (Response response = client.target("http://localhost:" + port + "/slow")
                    .request(MediaType.TEXT_PLAIN_TYPE).get()) {
                assertEquals(200, response.getStatus());
                assertEquals(SLOW_RESPONSE, response.readEntity(String.class));
            }
        }
    }
}
