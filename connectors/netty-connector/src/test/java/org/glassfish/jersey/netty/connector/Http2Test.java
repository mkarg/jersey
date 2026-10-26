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
import java.util.concurrent.TimeUnit;

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


/**
 * Tests the HTTP/2 (the cleartext HTTP/2 prior knowledge, i.e. h2c) support of the Netty connector.
 */
public class Http2Test {

    private static final String GET_RESPONSE = "Hello World!";
    private static final String POST_RESPONSE = "Posted!";

    private Channel serverChannel;
    private NioEventLoopGroup serverGroup;
    private int port;

    @BeforeEach
    public void startServer() throws Exception {
        serverGroup = new NioEventLoopGroup(1);
        ServerBootstrap b = new ServerBootstrap();
        b.group(serverGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
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

    /**
     * A minimal HTTP/2 server stream handler: echoes the request method in a response header and answers
     * with a fixed body.
     */
    @io.netty.channel.ChannelHandler.Sharable
    private static final class Http2ServerStreamHandler extends SimpleChannelInboundHandler<Object> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (!(msg instanceof Http2HeadersFrame)) {
                return;
            }
            final Http2HeadersFrame headersFrame = (Http2HeadersFrame) msg;
            final Http2Headers headers = headersFrame.headers();

            final boolean isPost = "POST".contentEquals(headers.method());
            final String body = isPost ? POST_RESPONSE : GET_RESPONSE;

            final Http2Headers response = new DefaultHttp2Headers()
                    .status("200")
                    .set(HttpHeaderNames.CONTENT_TYPE, MediaType.TEXT_PLAIN)
                    .set("x-http2-echo-method", String.valueOf(headers.method()));

            ctx.write(new DefaultHttp2HeadersFrame(response, false));
            final ByteBuf content = Unpooled.wrappedBuffer(body.getBytes(StandardCharsets.UTF_8));
            ctx.write(new DefaultHttp2DataFrame(content, true));
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
                assertEquals(POST_RESPONSE, response.readEntity(String.class));
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
}
