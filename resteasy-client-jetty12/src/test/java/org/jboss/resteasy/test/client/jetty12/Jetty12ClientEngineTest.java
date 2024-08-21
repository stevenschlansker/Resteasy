package org.jboss.resteasy.test.client.jetty12;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.client.ClientResponseFilter;
import jakarta.ws.rs.client.CompletionStageRxInvoker;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import org.apache.http.entity.ContentType;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.Callback;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.engines.jetty12.Jetty12ClientEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.jboss.resteasy.client.jaxrs.engines.jetty.AbstractJettyClientEngine.IDLE_TIMEOUT_MS;
import static org.jboss.resteasy.client.jaxrs.engines.jetty.AbstractJettyClientEngine.REQUEST_TIMEOUT_MS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class Jetty12ClientEngineTest {
    Server server = new Server(0);
    Client client;

    @AfterEach
    public void stop() throws Exception {
        if (client != null) {
            client.close();
        }
        server.stop();
    }

    private Client client() throws Exception {
        if (!server.isStarted()) {
            server.start();
        }
        if (client == null) {
            final HttpClient hc = new HttpClient();
            client = ((ResteasyClientBuilder) ClientBuilder.newBuilder()).httpEngine(new Jetty12ClientEngine(hc)).build();
        }
        return client;
    }

    @Test
    public void testSimple() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request,
                    final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                final var headers = request.getHeaders();
                if (headers.get("User-Agent").contains("Apache")) {
                    response.setStatus(503);
                    callback.succeeded();
                } else if (!"abracadabra".equals(headers.get("Password"))) {
                    response.setStatus(403);
                    callback.succeeded();
                } else {
                    response.setStatus(200);
                    response.write(true, UTF_8.encode("Success"), callback);
                }
                return true;
            }
        });

        final Response response = client().target(baseUri()).request()
                .header("Password", "abracadabra")
                .get();

        assertEquals(200, response.getStatus());
        assertEquals("Success", response.readEntity(String.class));
    }

    @Test
    public void testSimpleResponseRx() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request,
                    final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                final var headers = request.getHeaders();
                if (headers.get("User-Agent").contains("Apache")) {
                    response.setStatus(503);
                    callback.succeeded();
                } else if (!"abracadabra".equals(headers.get("Password"))) {
                    response.setStatus(403);
                    callback.succeeded();
                } else {
                    response.setStatus(200);
                    response.getHeaders().put(HttpHeader.CONTENT_TYPE, ContentType.TEXT_PLAIN.getMimeType());
                    response.write(true, UTF_8.encode("Success"), callback);
                }
                return true;
            }
        });

        final CompletionStage<Response> cs = client().target(baseUri()).request()
                .header("Password", "abracadabra").rx(CompletionStageRxInvoker.class)
                .get();

        final Response response = cs.toCompletableFuture().get();
        assertEquals(200, response.getStatus());
        assertEquals("Success", response.readEntity(String.class));
    }

    @Test
    public void testSimpleStringRx() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request,
                    final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                if (request.getHeaders().get("User-Agent").contains("Apache")) {
                    response.setStatus(503);
                } else if (!"abracadabra".equals(request.getHeaders().get("Password"))) {
                    response.setStatus(403);
                } else {
                    response.setStatus(200);
                    response.getHeaders().put(HttpHeader.CONTENT_TYPE, ContentType.TEXT_PLAIN.getMimeType());
                    response.write(true, UTF_8.encode("Success"), callback);
                }
                return true;
            }
        });

        final CompletionStage<String> cs = client().target(baseUri()).request()
                .header("Password", "abracadabra").rx(CompletionStageRxInvoker.class)
                .get(String.class);

        final String response = cs.toCompletableFuture().get();
        assertEquals("Success", response);
    }

    @Test
    public void testBigly() throws Exception {
        server.setHandler(new EchoHandler());
        final byte[] valuableData = randomAlpha().getBytes(StandardCharsets.UTF_8);
        final Response response = client().target(baseUri()).request()
                .post(Entity.entity(valuableData, MediaType.APPLICATION_OCTET_STREAM_TYPE));

        assertEquals(200, response.getStatus());
        assertArrayEquals(valuableData, response.readEntity(byte[].class));
    }

    @Test
    public void testFutureResponse() throws Exception {
        server.setHandler(new EchoHandler());
        final String valuableData = randomAlpha();
        final Future<Response> response = client().target(baseUri()).request()
                .buildPost(Entity.entity(valuableData, MediaType.APPLICATION_OCTET_STREAM_TYPE))
                .submit();

        final Response resp = response.get(10, TimeUnit.SECONDS);
        assertEquals(200, resp.getStatus());
        assertEquals(valuableData, resp.readEntity(String.class));
    }

    @Test
    public void testFutureString() throws Exception {
        server.setHandler(new EchoHandler());
        final String valuableData = randomAlpha();
        final Future<String> response = client().target(baseUri()).request()
                .buildPost(Entity.entity(valuableData, MediaType.APPLICATION_OCTET_STREAM_TYPE))
                .submit(String.class);

        final String result = response.get(10, TimeUnit.SECONDS);
        assertEquals(valuableData.length(), result.length());
        assertEquals(valuableData, result);
    }

    private String randomAlpha() {
        final StringBuilder builder = new StringBuilder();
        final Random r = new Random();
        for (int i = 0; i < 20 * 1024 * 1024; i++) {
            builder.append((char) ('a' + (char) r.nextInt('z' - 'a')));
            if (i % 100 == 0)
                builder.append('\n');
        }
        return builder.toString();
    }

    @Test
    public void testTimeout() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request,
                    final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                callback.succeeded();
                return true;
            }
        });

        try {
            client().target(baseUri()).request()
                    .property(REQUEST_TIMEOUT_MS, Duration.ofMillis(500))
                    .get();
            fail();
        } catch (final ProcessingException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testIdleTimeout() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request,
                    final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                response.setStatus(200);
                response.write(true, UTF_8.encode("Success"), callback);
                return true;
            }
        });

        try {
            client().target(baseUri()).request()
                    .property(REQUEST_TIMEOUT_MS, Duration.ofMillis(2000))
                    .property(IDLE_TIMEOUT_MS, Duration.ofMillis(500))
                    .get();
            fail();
        } catch (final ProcessingException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }

        final Response response = client().target(baseUri()).request()
                .property(REQUEST_TIMEOUT_MS, Duration.ofMillis(2000))
                .property(IDLE_TIMEOUT_MS, Duration.ofMillis(1500))
                .get();

        assertEquals(200, response.getStatus());
        assertEquals("Success", response.readEntity(String.class));

    }

    @Test
    public void testDeferContent() throws Exception {
        server.setHandler(new EchoHandler());
        final byte[] valuableData = randomAlpha().getBytes(StandardCharsets.UTF_8);
        final Response response = client().target(baseUri()).request()
                .post(Entity.entity(new StreamingOutput() {
                    @Override
                    public void write(final OutputStream output) throws IOException, WebApplicationException {
                        try {
                            Thread.sleep(100);
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                        output.write(valuableData);
                    }
                }, MediaType.APPLICATION_OCTET_STREAM_TYPE));

        assertEquals(200, response.getStatus());
        assertArrayEquals(valuableData, response.readEntity(byte[].class));
    }

    @Test
    public void testFilterBufferReplay() throws Exception {
        final String greeting = "Success";
        final byte[] expected = greeting.getBytes(StandardCharsets.UTF_8);
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request,
                    final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                response.setStatus(200);
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, MediaType.TEXT_PLAIN);
                response.write(true, UTF_8.encode(greeting), callback);
                return true;
            }
        });

        final byte[] content = new byte[expected.length];
        final ClientResponseFilter capturer = new ClientResponseFilter() {
            @Override
            public void filter(final ClientRequestContext requestContext, final ClientResponseContext responseContext)
                    throws IOException {
                responseContext.getEntityStream().read(content);
            }
        };

        try (InputStream response = client().register(capturer).target(baseUri()).request()
                .get(InputStream.class)) {
            // ignored, we are checking filter
        }

        assertArrayEquals(expected, content);
    }

    public URI baseUri() {
        return URI.create("http://localhost:" + ((ServerConnector) server.getConnectors()[0]).getLocalPort());
    }

    static class EchoHandler extends Handler.Abstract {
        @Override
        public boolean handle(final Request request,
                final org.eclipse.jetty.server.Response response, final Callback callback)
                throws Exception {
            String type = request.getHeaders().get(HttpHeader.CONTENT_TYPE);
            if (type == null) {
                type = MediaType.TEXT_PLAIN;
            }

            response.getHeaders().put(HttpHeader.CONTENT_TYPE, type);
            response.setStatus(200);
            Content.copy(request, response, callback);
            return true;
        }
    }
}
