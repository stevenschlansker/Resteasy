package org.jboss.resteasy.client.jaxrs.engines.jetty12;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

import jakarta.ws.rs.client.InvocationCallback;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.InputStreamResponseListener;
import org.eclipse.jetty.client.OutputStreamRequestContent;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.Request.Content;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.http.HttpFields;
import org.jboss.resteasy.client.jaxrs.engines.jetty.AbstractJettyClientEngine;
import org.jboss.resteasy.client.jaxrs.internal.ClientInvocation;
import org.jboss.resteasy.client.jaxrs.internal.ClientResponse;

public class Jetty12ClientEngine extends AbstractJettyClientEngine<Request> {

    private final HttpClient client;

    public Jetty12ClientEngine(final HttpClient client) {
        if (!client.isStarted()) {
            try {
                client.start();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
        this.client = client;
    }

    @Override
    public SSLContext getSslContext() {
        return client.getSslContextFactory().getSslContext();
    }

    @Override
    public void close() {
        try {
            client.stop();
        } catch (final Exception e) {
            throw new RuntimeException("Unable to close JettyHttpEngine", e);
        }
    }

    @Override
    protected Request newRequest(final ClientInvocation invocation) {
        return client.newRequest(invocation.getUri())
                .method(invocation.getMethod());
    }

    @Override
    protected <T> void sendRequest(final ClientInvocation invocation, final Request request,
            final InvocationCallback<T> callback, final CompletableFuture<T> future,
            final boolean buffered, final ResultExtractor<T> extractor) {

        request.send(new InputStreamResponseListener() {
            private ClientResponse cr;

            @Override
            public void onHeaders(final Response response) {
                super.onHeaders(response);
                final InputStream inputStream = getInputStream();
                cr = onResponseHeaders(invocation, inputStream, buffered, extractor,
                        callback, future, response.getStatus(), response.getHeaders(),
                        failure -> onFailure(response, failure));
            }

            @Override
            public void onFailure(final Response response, final Throwable failure) {
                super.onFailure(response, failure);
                failed(cr, future, callback, failure);
            }
        });
    }

    @Override
    protected void headers(final Request request, final Consumer<BiConsumer<String, String>> addHeaders) {
        request.headers(mutable -> addHeaders.accept(mutable::add));
    }

    @Override
    protected ContentOut createContentOut(final String mediaType) {
        return new ContentOut() {
            @SuppressWarnings("resource")
            private final OutputStreamRequestContent entity = new OutputStreamRequestContent(mediaType);

            @Override
            public void close() throws IOException {
                entity.close();
            }

            @Override
            public OutputStream outputStream() {
                return entity.getOutputStream();
            }

            @Override
            public Object entity() {
                return entity;
            }
        };
    }

    MultivaluedMap<String, String> extract(final HttpFields headers) {
        final MultivaluedMap<String, String> extracted = new MultivaluedHashMap<>();
        headers.forEach(h -> extracted.add(h.getName(), h.getValue()));
        return extracted;
    }

    @Override
    protected Map<String, Object> getAttributes(final Request request) {
        return request.getAttributes();
    }

    @Override
    protected void setTimeout(final Request request, final long timeoutMs) {
        request.timeout(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void setIdleTimeout(final Request request, final long idleTimeoutMs) {
        request.idleTimeout(idleTimeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void setFollowRedirects(final Request request, final boolean followRedirects) {
        request.followRedirects(followRedirects);
    }

    @Override
    protected void requestEntity(final Request request, final Object entity) {
        request.body((Content) entity);
    }

    @Override
    protected void setAttribute(final Request request, final String k, final Object v) {
        request.attribute(k, v);
    }

    @Override
    protected void abortRequest(final Request request, final Exception cancel) {
        request.abort(cancel);
    }
}
