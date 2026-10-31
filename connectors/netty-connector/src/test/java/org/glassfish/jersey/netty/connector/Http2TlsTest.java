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
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import javax.net.ssl.SSLContext;
import org.glassfish.jersey.client.ClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the HTTPS (TLS ALPN) paths of the HTTP/2 support of the Netty connector: the successful
 * negotiation of the HTTP/2 protocol and the fallback to the HTTP/1.1 pipeline when the server
 * does not support HTTP/2.
 */
public class Http2TlsTest {

    private static final String TLS_RESPONSE = "Hello TLS World!";

    private Channel serverChannel;
    private NioEventLoopGroup serverGroup;
    private int port;

    @BeforeEach
    public void startServer() throws Exception {
        serverGroup = new NioEventLoopGroup(1);
        serverChannel = httpsServer(Http2FrameCodecBuilder.forServer().build(), true);
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
     * Starts a TLS server with the given connection handler. When {@code withH2} is set, the server
     * advertises the HTTP/2 application protocol in the ALPN, otherwise it offers HTTP/1.1 only.
     */
    private Channel httpsServer(Object http2Handler, boolean withH2) throws Exception {
        final SelfSignedCertificate certificate = new SelfSignedCertificate();
        SslContext sslContext;
        if (withH2) {
            sslContext = SslContextBuilder.forServer(certificate.certificate(), certificate.privateKey())
                    .sslProvider(SslProvider.JDK)
                    .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
                    .applicationProtocolConfig(new ApplicationProtocolConfig(
                            ApplicationProtocolConfig.Protocol.ALPN,
                            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                            ApplicationProtocolNames.HTTP_2,
                            ApplicationProtocolNames.HTTP_1_1))
                    .build();
        } else {
            sslContext = SslContextBuilder.forServer(certificate.certificate(), certificate.privateKey())
                    .sslProvider(SslProvider.JDK)
                    .build();
        }

        ServerBootstrap b = new ServerBootstrap();
        b.group(serverGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
                 if (http2Handler != null) {
                     ch.pipeline().addLast(http2Handler, new Http2MultiplexHandler(new TlsServerStreamHandler()));
                 } else {
                     ch.pipeline().addLast(new HttpServerCodec(),
                             new HttpObjectAggregator(1 << 20),
                             new Http1TlsServerHandler());
                 }
             }
         });
        return b.bind(0).sync().channel();
    }

    /** A minimal HTTP/2 server stream handler answering with a fixed body. */
    @io.netty.channel.ChannelHandler.Sharable
    private static final class TlsServerStreamHandler extends SimpleChannelInboundHandler<Object> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final Object msg) {
            if (!(msg instanceof Http2HeadersFrame)) {
                return;
            }
            final Http2Headers headers = ((Http2HeadersFrame) msg).headers();
            final Http2Headers response = new DefaultHttp2Headers()
                    .status("200")
                    .set(HttpHeaderNames.CONTENT_TYPE, MediaType.TEXT_PLAIN);
            ctx.write(new DefaultHttp2HeadersFrame(response, false));
            ctx.write(new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(
                    TLS_RESPONSE.getBytes(StandardCharsets.UTF_8)), true));
            ctx.flush();
        }
    }

    /** A minimal HTTP/1.1 server handler answering with a fixed body. */
    private static final class Http1TlsServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest req) {
            final DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.wrappedBuffer(TLS_RESPONSE.getBytes(StandardCharsets.UTF_8)));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, MediaType.TEXT_PLAIN);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private Client httpsClient(boolean enableHttp2) throws CertificateException {
        ClientConfig config = new ClientConfig();
        config.connectorProvider(new NettyConnectorProvider());
        if (enableHttp2) {
            config.property(NettyClientProperties.ENABLE_HTTP2, Boolean.TRUE);
        }
        // the self-signed test certificate is not in the default trust store
        config.property(NettyClientProperties.ENABLE_SSL_HOSTNAME_VERIFICATION, Boolean.FALSE);
        return ClientBuilder.newBuilder()
                .withConfig(config)
                .sslContext(createTrustAllSslContext())
                .build();
    }

    private static SSLContext createTrustAllSslContext() throws CertificateException {
        try {
            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            throw new CertificateException(e);
        }
    }

    /**
     * With the HTTP/2 enabled, the TLS ALPN negotiates the HTTP/2 protocol and the request succeeds.
     */
    @Test
    public void testHttpsAlpnHttp2() throws Exception {
        try (Client client = httpsClient(true)) {
            try (Response response = client.target("https://localhost:" + port + "/helloworld")
                    .request(MediaType.TEXT_PLAIN_TYPE).get()) {
                assertEquals(200, response.getStatus());
                assertEquals(TLS_RESPONSE, response.readEntity(String.class));
            }
        }
    }

    /**
     * When the TLS server does not offer the HTTP/2 application protocol, the client falls back to
     * the HTTP/1.1 pipeline and the request still succeeds.
     */
    @Test
    public void testHttpsAlpnFallbackToHttp1() throws Exception {
        // a TLS server without the HTTP/2 support (no ALPN, plain HTTP/1.1 handler)
        if (serverChannel != null) {
            serverChannel.close();
        }
        serverChannel = httpsServer(null, false);
        port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

        try (Client client = httpsClient(true)) {
            try (Response response = client.target("https://localhost:" + port + "/helloworld")
                    .request(MediaType.TEXT_PLAIN_TYPE).get()) {
                assertEquals(200, response.getStatus());
                assertEquals(TLS_RESPONSE, response.readEntity(String.class));
            }
        }
    }
}
