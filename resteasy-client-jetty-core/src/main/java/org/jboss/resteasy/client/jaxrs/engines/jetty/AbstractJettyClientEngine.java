package org.jboss.resteasy.client.jaxrs.engines.jetty;

import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.net.ssl.HostnameVerifier;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.InvocationCallback;
import jakarta.ws.rs.client.ResponseProcessingException;
import jakarta.ws.rs.core.EntityPart;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;

import org.eclipse.jetty.http.HttpFields;
import org.jboss.logging.Logger;
import org.jboss.resteasy.client.jaxrs.engines.AsyncClientHttpEngine;
import org.jboss.resteasy.client.jaxrs.internal.ClientInvocation;
import org.jboss.resteasy.client.jaxrs.internal.ClientResponse;

public abstract class AbstractJettyClientEngine<JettyRequest> implements AsyncClientHttpEngine {

    private static final Logger LOGGER = Logger.getLogger(AbstractJettyClientEngine.class);
    private static final MediaType MULTIPART_WILDCARD = new MediaType("multipart", "*");
    private static final Class<?> MULTIPART_OUTPUT;

    static {
        // Check if the org.jboss.resteasy.plugins.providers.multipart.MultipartOutput is on the class path
        final String className = "org.jboss.resteasy.plugins.providers.multipart.MultipartOutput";
        Class<?> multipartOutput = null;
        try {
            multipartOutput = Class.forName(className, false, resolveClassLoader());
        } catch (final ClassNotFoundException e) {
            LOGGER.tracef(e, "Failed to load %s", className);
        }

        MULTIPART_OUTPUT = multipartOutput;
    }

    private static final String BASE_PROP = "org.jboss.resteasy.client.jaxrs.engines.jetty.JettyClientEngine";
    public static final String REQUEST_TIMEOUT_MS = BASE_PROP + "$RequestTimeout";
    public static final String IDLE_TIMEOUT_MS = BASE_PROP + "$IdleTimeout";
    // Yeah, this is the Jersey one, but there's no standard one and it makes more sense to reuse than make our own...
    public static final String FOLLOW_REDIRECTS = "jersey.config.client.followRedirects";

    private static final InvocationCallback<ClientResponse> NOP = new InvocationCallback<ClientResponse>() {
        @Override
        public void completed(final ClientResponse response) {
        }

        @Override
        public void failed(final Throwable throwable) {
        }
    };

    @Override
    public HostnameVerifier getHostnameVerifier() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClientResponse invoke(final Invocation invocation) {
        final Future<ClientResponse> future = submit((ClientInvocation) invocation, false, NOP, null);
        try {
            return future.get(1, TimeUnit.HOURS); // There's already an idle and connect timeout, do we need one here?
        } catch (final InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw clientException(e, null);
        } catch (TimeoutException | ExecutionException e) {
            future.cancel(true);
            throw clientException(e.getCause(), null);
        }
    }

    @Override
    public <T> Future<T> submit(final ClientInvocation invocation, final boolean bufIn, final InvocationCallback<T> callback,
            final ResultExtractor<T> extractor) {
        return doSubmit(invocation, bufIn, callback, extractor);
    }

    @Override
    public <T> CompletableFuture<T> submit(final ClientInvocation request, final boolean buffered,
            final ResultExtractor<T> extractor,
            final ExecutorService executorService) {
        return doSubmit(request, buffered, null, extractor);
    }

    private <T> CompletableFuture<T> doSubmit(final ClientInvocation invocation, final boolean buffered,
            final InvocationCallback<T> callback,
            final ResultExtractor<T> extractor) {
        final ExecutorService asyncExecutor = invocation.asyncInvocationExecutor();

        final JettyRequest request = newRequest(invocation);
        final CompletableFuture<T> future = new RequestFuture<T>(request);

        // Determine if this is a multipart request
        final Object entity = invocation.getEntity();
        final boolean addBoundary = isMultipart(invocation) && canSetBoundary(entity);

        invocation.getMutableProperties().forEach((k, v) -> setAttribute(request, k, v));
        headers(request, setHeader -> invocation.getHeaders().asMap()
                .forEach((h, vs) -> vs.forEach(v -> {
                    String headerValue = v;
                    if (addBoundary && h.equalsIgnoreCase("content-type")) {
                        final MediaType mediaType = MediaType.valueOf(v);
                        // Set the boundary if needed
                        if (mediaType.getParameters().get("boundary") == null) {
                            headerValue = headerValue + "; boundary=" + UUID.randomUUID();
                            // Replace the MediaType on the invocation if we've added a boundary
                            invocation.getHeaders().setMediaType(MediaType.valueOf(headerValue));
                        }
                    }
                    setHeader.accept(h, headerValue);
                })));
        configureTimeout(request);
        if (getAttributes(request).get(FOLLOW_REDIRECTS) == Boolean.FALSE) {
            setFollowRedirects(request, false);
        }

        if (entity != null) {
            final ContentOut contentOut = createContentOut(
                    Objects.toString(invocation.getHeaders().getMediaType(), null));
            asyncExecutor.execute(() -> {
                try {
                    try (OutputStream bodyOut = contentOut.outputStream()) {
                        invocation.writeRequestBody(bodyOut);
                    }
                } catch (final Exception e) { // Also catch any exception thrown from close
                    future.completeExceptionally(e);
                    if (callback != null) {
                        callback.failed(e);
                    }
                }
            });
            requestEntity(request, contentOut.entity());
        }

        sendRequest(invocation, request, callback, future, buffered, extractor);
        return future;
    }

    private void configureTimeout(final JettyRequest request) {
        final Map<String, Object> attributes = getAttributes(request);
        final Object timeout = attributes.get(REQUEST_TIMEOUT_MS);
        final Object idleTimeout = attributes.get(IDLE_TIMEOUT_MS);
        final long timeoutMs = parseTimeoutMs(timeout);
        final long idleTimeoutMs = parseTimeoutMs(idleTimeout);
        if (timeoutMs > 0) {
            setTimeout(request, timeoutMs);
        }

        if (idleTimeoutMs > 0) {
            setIdleTimeout(request, idleTimeoutMs);
        }
    }

    protected abstract JettyRequest newRequest(ClientInvocation invocation);

    protected abstract void headers(JettyRequest request, Consumer<BiConsumer<String, String>> addHeaders);

    protected abstract ContentOut createContentOut(String mediaType);

    protected abstract void requestEntity(JettyRequest request, Object entity);

    protected abstract Map<String, Object> getAttributes(JettyRequest request);

    protected abstract void setAttribute(JettyRequest request, String k, Object v);

    protected abstract void setTimeout(JettyRequest request, long timeoutMs);

    protected abstract void setIdleTimeout(JettyRequest request, long idleTimeoutMs);

    protected abstract void setFollowRedirects(JettyRequest request, boolean followRedirects);

    protected abstract <T> void sendRequest(ClientInvocation invocation, JettyRequest request,
            InvocationCallback<T> callback, CompletableFuture<T> future,
            boolean buffered, ResultExtractor<T> extractor);

    @SuppressWarnings("unchecked")
    protected <T> ClientResponse onResponseHeaders(final ClientInvocation invocation,
            final InputStream inputStream, final boolean buffered, final ResultExtractor<T> extractor,
            final InvocationCallback<T> callback, final CompletableFuture<T> future,
            final int status, final HttpFields httpFields,
            final Consumer<Throwable> onFailure) {
        final var cr = new JettyClientResponse(invocation.getClientConfiguration(), inputStream);
        cr.setProperties(invocation.getMutableProperties());
        cr.setStatus(status);
        final MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        httpFields.forEach(h -> headers.add(h.getName(), h.getValue()));
        cr.setHeaders(headers);
        invocation.asyncInvocationExecutor().submit(() -> {
            try {
                if (buffered) {
                    cr.bufferEntity();
                }
                complete(future, callback, extractor == null ? (T) cr : extractor.extractResult(cr));
            } catch (final Exception e) {
                try {
                    inputStream.close();
                } catch (final Exception e1) {
                    e.addSuppressed(e1);
                }
                onFailure.accept(e);
            }
        });
        return cr;
    }

    protected <T> void complete(final CompletableFuture<T> future, final InvocationCallback<T> callback, final T result) {
        future.complete(result);
        if (callback != null) {
            callback.completed(result);
        }
    }

    protected void failed(final ClientResponse cr, final CompletableFuture<?> future, final InvocationCallback<?> callback,
            final Throwable t) {
        final RuntimeException x = clientException(t, cr);
        future.completeExceptionally(x);
        if (callback != null) {
            callback.failed(x);
        }
    }

    private long parseTimeoutMs(final Object timeout) {
        final long timeoutMs;
        if (timeout instanceof Duration) {
            timeoutMs = ((Duration) timeout).toMillis();
        } else if (timeout instanceof Number) {
            timeoutMs = ((Number) timeout).intValue();
        } else if (timeout != null) {
            timeoutMs = Integer.parseInt(timeout.toString());
        } else {
            timeoutMs = -1;
        }
        return timeoutMs;
    }

    protected abstract void abortRequest(JettyRequest request, Exception cancel);

    protected static RuntimeException clientException(final Throwable ex, final jakarta.ws.rs.core.Response clientResponse) {
        RuntimeException ret;
        if (ex == null) {
            final NullPointerException e = new NullPointerException();
            e.fillInStackTrace();
            ret = new ProcessingException(e);
        } else if (ex instanceof WebApplicationException) {
            ret = (WebApplicationException) ex;
        } else if (ex instanceof ProcessingException) {
            ret = (ProcessingException) ex;
        } else if (clientResponse != null) {
            ret = new ResponseProcessingException(clientResponse, ex);
        } else {
            ret = new ProcessingException(ex);
        }
        ret.fillInStackTrace();
        return ret;
    }

    protected static ClassLoader resolveClassLoader() {
        if (System.getSecurityManager() == null) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            if (cl == null) {
                cl = AbstractJettyClientEngine.class.getClassLoader();
            }
            return cl == null ? ClassLoader.getSystemClassLoader() : cl;
        }
        return AccessController.doPrivileged((PrivilegedAction<ClassLoader>) () -> {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            if (cl == null) {
                cl = AbstractJettyClientEngine.class.getClassLoader();
            }
            return cl == null ? ClassLoader.getSystemClassLoader() : cl;
        });
    }

    private static boolean isMultipart(final ClientInvocation invocation) {
        return MULTIPART_WILDCARD.isCompatible(invocation.getHeaders().getMediaType());
    }

    private static boolean canSetBoundary(final Object entity) {
        if (MULTIPART_OUTPUT != null && MULTIPART_OUTPUT.isInstance(entity)) {
            return true;
        }
        if (entity instanceof EntityPart) {
            return true;
        }
        if (entity instanceof List<?>) {
            // We're a list, if we're not empty check the first type to see if it's an entity part
            final List<?> list = (List<?>) entity;
            if (!list.isEmpty()) {
                return list.get(0) instanceof EntityPart;
            }
        }
        return false;
    }

    public interface ContentOut extends Closeable {
        OutputStream outputStream();

        Object entity();
    }

    class RequestFuture<T> extends CompletableFuture<T> {
        private final JettyRequest request;

        RequestFuture(final JettyRequest request) {
            this.request = request;
        }

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            final boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (mayInterruptIfRunning && cancelled) {
                abortRequest(request, new CancellationException());
            }
            return cancelled;
        }
    }

    protected abstract class SentRequest<T> {
        protected CompletableFuture<T> future;
    }
}
