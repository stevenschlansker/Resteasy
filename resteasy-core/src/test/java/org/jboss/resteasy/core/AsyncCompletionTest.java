/*
 * Copyright The RESTEasy Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.jboss.resteasy.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.NewCookie;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import jakarta.ws.rs.ext.Provider;

import org.jboss.resteasy.core.providerfactory.ResteasyProviderFactoryImpl;
import org.jboss.resteasy.mock.MockHttpRequest;
import org.jboss.resteasy.mock.MockHttpResponse;
import org.jboss.resteasy.plugins.providers.StreamingOutputProvider;
import org.jboss.resteasy.plugins.providers.StringTextStar;
import org.jboss.resteasy.resteasy_jaxrs.i18n.LogMessages;
import org.jboss.resteasy.specimpl.BuiltResponse;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.jboss.resteasy.spi.AsyncMessageBodyWriter;
import org.jboss.resteasy.spi.AsyncOutputStream;
import org.jboss.resteasy.spi.BlockingAsyncOutputStream;
import org.jboss.resteasy.spi.Dispatcher;
import org.jboss.resteasy.spi.ResteasyAsynchronousContext;
import org.jboss.resteasy.spi.ResteasyAsynchronousResponse;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.jboss.resteasy.spi.RunnableWithException;
import org.jboss.resteasy.spi.UnhandledException;
import org.jboss.resteasy.tracing.RESTEasyTracingLogger;
import org.jboss.resteasy.tracing.api.RESTEasyTracing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The completion callback of an asynchronous response hands the request back to the container. A servlet container
 * can then recycle the request and the response, and give the response to the next request on the connection. After
 * the callback runs, RESTEasy must not use either of them.
 * <p>
 * The mocks model one outcome of that race. After the recycle, a read of the request throws, as in Jetty 12 before
 * the next request arrives, and the response stands for the response of the next request: it is not committed, and
 * it records each call that would change it.
 */
public class AsyncCompletionTest {

    private ResteasyProviderFactory providerFactory;
    private SynchronousDispatcher dispatcher;
    private MockHttpRequest request;
    private RecycledResponse response;
    private ResteasyAsynchronousResponse asyncResponse;
    private List<String> callsAfterRecycle;
    private List<Throwable> completions;
    private volatile boolean recycled;

    @BeforeEach
    public void setUp() throws Exception {
        providerFactory = new ResteasyProviderFactoryImpl();
        providerFactory.registerProvider(StringTextStar.class);
        providerFactory.registerProvider(StreamingOutputProvider.class);
        dispatcher = new SynchronousDispatcher(providerFactory);
        callsAfterRecycle = Collections.synchronizedList(new ArrayList<>());
        completions = Collections.synchronizedList(new ArrayList<>());
        request = spy(MockHttpRequest.get("/"));
        doAnswer(invocation -> {
            if (recycled) {
                callsAfterRecycle.add("getAttribute(" + invocation.getArgument(0) + ")");
                throw new NullPointerException("the request is recycled");
            }
            return invocation.callRealMethod();
        }).when(request).getAttribute(anyString());
        response = new RecycledResponse();
        ResteasyContext.pushContext(Dispatcher.class, dispatcher);
        dispatcher.pushContextObjects(request, response);
    }

    @AfterEach
    public void clearContext() {
        ResteasyContext.clearContextData();
    }

    @Test
    public void anExceptionResponseDoesNotUseTheRequestAfterCompletion() {
        suspend();

        dispatcher.asynchronousExceptionDelivery(request, response, new ForbiddenException(), recycle());

        assertEquals(403, response.statusBeforeRecycle);
        assertEquals(Collections.singletonList(null), completions);
        assertEquals(Collections.emptyList(), callsAfterRecycle);
    }

    @Test
    public void anExceptionResponseDoesNotWriteAnExceptionThatFollowsCompletion() {
        suspend();

        dispatcher.asynchronousExceptionDelivery(request, response, new ForbiddenException(), recycleAndThrow());

        assertEquals(Collections.singletonList(null), completions);
        assertEquals(Collections.emptyList(), callsAfterRecycle);
    }

    @Test
    public void anExceptionResponseCompletesWhenTheReportOfAWriteFailureThrows() {
        suspend();
        response.resetFails = true;
        Response unwritable = Response.status(403).entity(new Object()).type("application/x-unwritable").build();

        dispatcher.asynchronousExceptionDelivery(request, response, new WebApplicationException(unwritable), recycle());

        assertEquals(Collections.singletonList(null), completions);
        assertEquals(Collections.emptyList(), callsAfterRecycle);
    }

    @Test
    public void anEmptyBodyCommitsItsHeadersBeforeCompletion() {
        suspend();
        Response forbidden = Response.status(403).header("X-Committed", "yes")
                .entity((StreamingOutput) out -> {
                }).type(MediaType.TEXT_PLAIN_TYPE).build();

        dispatcher.asynchronousExceptionDelivery(request, response, new WebApplicationException(forbidden), recycle());

        assertEquals(Collections.emptyList(), callsAfterRecycle);
        assertEquals("yes", response.headers().getFirst("X-Committed"));
    }

    @Test
    public void aFailedHeaderCommitReachesTheCallbackAndTheCaller() {
        response.cookieFails = true;
        Response forbidden = Response.status(403).cookie(new NewCookie.Builder("c").value("v").build())
                .entity((StreamingOutput) out -> {
                }).type(MediaType.TEXT_PLAIN_TYPE).build();

        UnhandledException thrown = assertThrows(UnhandledException.class,
                () -> dispatcher.writeException(request, response, new WebApplicationException(forbidden),
                        completions::add));

        assertEquals(1, completions.size());
        assertSame(completions.get(0), thrown.getCause());
    }

    @Test
    public void aFailureBeforeTheWriterIsTraced() {
        providerFactory.register((ContainerResponseFilter) (requestContext, responseContext) -> {
            throw new IllegalStateException("from a response filter");
        });

        List<String> events = traced(() -> assertThrows(UnhandledException.class,
                () -> dispatcher.writeException(request, response, forbiddenWithEntity(), completions::add)));

        assertFalse(events.isEmpty(), "no tracing events in the response headers");
        assertTrue(events.get(events.size() - 1).contains("FINISHED"), () -> "FINISHED is not last in " + events);
    }

    @Test
    public void anErrorBeforeTheWriterIsTracedAndReachesTheCaller() {
        LinkageError error = new LinkageError("from a response filter");
        providerFactory.register((ContainerResponseFilter) (requestContext, responseContext) -> {
            throw error;
        });

        List<String> events = traced(() -> assertSame(error, assertThrows(LinkageError.class,
                () -> dispatcher.writeException(request, response, forbiddenWithEntity(), completions::add))));

        assertFalse(events.isEmpty(), "no tracing events in the response headers");
        assertTrue(events.get(events.size() - 1).contains("FINISHED"), () -> "FINISHED is not last in " + events);
    }

    @Test
    public void aWriteThatSuspendsCompletesWhenItsHeaderCommitFails() {
        CompletableFuture<Void> pending = new CompletableFuture<>();
        providerFactory.register(new PendingWriter(pending));
        AtomicBoolean suspended = new AtomicBoolean();
        ResteasyAsynchronousContext context = servletAsyncContext(suspended::get);
        // Suspend as Servlet3AsyncHttpRequest does when an asynchronous write is still in progress.
        when(context.executeAsyncIo(any())).thenAnswer(invocation -> {
            CompletableFuture<Void> write = invocation.<CompletionStage<Void>> getArgument(0).toCompletableFuture();
            if (!write.isDone())
                suspended.set(true);
            return write;
        });
        request.setAsynchronousContext(context);
        response.cookieFails = true;
        response.asyncOutput = true;
        dispatcher.pushContextObjects(request, response);

        dispatcher.writeResponse(request, response, Response.ok(new Pending()).type(MediaType.TEXT_PLAIN_TYPE)
                .cookie(new NewCookie.Builder("c").value("v").build()).build());
        assertTrue(suspended.get(), "the write did not suspend the request");
        pending.complete(null);

        verify(context, times(1)).complete();
    }

    @Test
    public void aWriteThatFinishesOnAnotherThreadDoesNotRethrowItsHeaderCommitFailure() {
        response.cookieFails = true;
        response.asyncOutput = true;
        providerFactory.register(new PendingWriter(new CompletesOnAnotherThread()));
        request.setAsynchronousContext(servletAsyncContext(() -> false));
        dispatcher.pushContextObjects(request, response);
        BuiltResponse pending = (BuiltResponse) Response.ok(new Pending()).type(MediaType.TEXT_PLAIN_TYPE)
                .cookie(new NewCookie.Builder("c").value("v").build()).build();

        assertDoesNotThrow(
                () -> ServerResponseWriter.writeNomapResponse(pending, request, response, providerFactory, recycle()));

        assertEquals(1, completions.size());
        assertTrue(completions.get(0) instanceof IllegalArgumentException, () -> "callback got " + completions);
        assertEquals(Collections.emptyList(), callsAfterRecycle);
    }

    @Test
    public void aCompletionStageWriteFailureIsLoggedAtDebugAfterCompletion() {
        suspend();
        AsyncResponseConsumer consumer = consumer();
        Response failing = Response.ok((StreamingOutput) out -> {
            throw new IOException("write failed");
        }).type(MediaType.TEXT_PLAIN_TYPE).build();

        assertEquals(List.of("DEBUG"), LogCapture.lateExceptions(() -> consumer.internalResume(failing, x -> {
        })));
        verify(asyncResponse, times(1)).complete();
    }

    @Test
    public void aCompletionStageExceptionResponseWriteFailureIsLoggedAtError() {
        suspend();
        AsyncResponseConsumer consumer = consumer();
        Response failing = Response.status(403).entity((StreamingOutput) out -> {
            throw new IOException("write failed");
        }).type(MediaType.TEXT_PLAIN_TYPE).build();

        assertEquals(List.of("ERROR"), LogCapture.lateExceptions(
                () -> consumer.internalResume(new WebApplicationException(failing), x -> consumer.complete(x))));
    }

    @Test
    public void aLateExceptionIsLoggedAtDebugOnlyWhenTheWriterCallbackReportedIt() {
        IOException writeFailure = new IOException("connection reset");
        AsyncCompletion written = AsyncCompletion.whenWritten();
        written.onWritten(t -> {
        }).accept(writeFailure);
        AsyncCompletion handedBack = AsyncCompletion.when(() -> true, false);
        handedBack.onWritten(t -> {
        }).accept(writeFailure);

        assertEquals(List.of("DEBUG"),
                LogCapture.lateExceptions(() -> written.afterCompletion(new UnhandledException(writeFailure))));
        assertEquals(List.of("ERROR"),
                LogCapture.lateExceptions(() -> written.afterCompletion(new IllegalStateException("late"))));
        assertEquals(List.of("ERROR"),
                LogCapture.lateExceptions(() -> handedBack.afterCompletion(new UnhandledException(writeFailure))));
        assertEquals(List.of(),
                LogCapture.lateExceptions(() -> AsyncCompletion.when(() -> false, false).afterCompletion(writeFailure)));
    }

    @Test
    public void aSynchronousExceptionResponseTracesFinishedLast() {
        assertFinishedIsLast(() -> dispatcher.writeException(request, response, forbiddenWithEntity(), completions::add));
    }

    @Test
    public void aSuspendedExceptionResponseTracesFinishedLast() {
        suspend();

        assertFinishedIsLast(() -> dispatcher.asynchronousExceptionDelivery(request, response, forbiddenWithEntity(),
                recycle()));

        assertEquals(Collections.emptyList(), callsAfterRecycle);
    }

    @Test
    public void aResumedEntityDoesNotWriteAnExceptionFromACompletionCallback() {
        suspend();
        AbstractAsynchronousResponse resumed = abstractAsynchronousResponse();
        resumed.register((CompletionCallback) t -> {
            throw new IllegalStateException("from a completion callback");
        });

        resumed.internalResume(Response.noContent().build(), recycle());

        assertEquals(204, response.statusBeforeRecycle);
        assertEquals(Collections.singletonList(null), completions);
        assertEquals(Collections.emptyList(), callsAfterRecycle);
    }

    @Test
    public void aResumedEntityRunsTheCompletionCallbacksWhenCompletionThrows() {
        suspend();
        AbstractAsynchronousResponse resumed = abstractAsynchronousResponse();
        List<Throwable> callbacks = Collections.synchronizedList(new ArrayList<>());
        resumed.register((CompletionCallback) callbacks::add);

        resumed.internalResume(Response.noContent().build(), recycleAndThrow());

        assertEquals(1, callbacks.size());
        assertEquals(Collections.emptyList(), callsAfterRecycle);
    }

    @Test
    public void aCompletionStageExceptionDoesNotWriteAnExceptionThatFollowsCompletion() {
        suspend();
        recycleAndThrowOnComplete();
        AsyncResponseConsumer consumer = consumer();

        consumer.internalResume(new ForbiddenException(), x -> consumer.complete(null));

        assertEquals(403, response.statusBeforeRecycle);
        assertEquals(Collections.emptyList(), callsAfterRecycle);
    }

    @Test
    public void aCompletionStageEntityDoesNotWriteAnExceptionThatFollowsCompletion() {
        suspend();
        recycleAndThrowOnComplete();
        AsyncResponseConsumer consumer = consumer();

        consumer.internalResume((Object) null, x -> consumer.complete(null));

        assertEquals(204, response.statusBeforeRecycle);
        assertEquals(Collections.emptyList(), callsAfterRecycle);
    }

    @Test
    public void aStreamElementThatFailsAfterItsWriteCompletesTheStream() {
        suspend();
        AsyncResponseConsumer consumer = consumer();

        consumer.internalResume((Object) null, x -> {
            completions.add(x);
            if (completions.size() == 1) {
                throw new IllegalStateException("from the next stream request");
            }
        });

        verify(asyncResponse, times(1)).complete();
    }

    private void suspend() {
        asyncResponse = mock(ResteasyAsynchronousResponse.class);
        ResteasyAsynchronousContext context = servletAsyncContext(() -> true);
        when(context.getAsyncResponse()).thenReturn(asyncResponse);
        request.setAsynchronousContext(context);
        dispatcher.pushContextObjects(request, response);
    }

    /**
     * @return an asynchronous context that writes as Servlet3AsyncHttpRequest does: blocking IO runs at once on the
     *         calling thread
     */
    private static ResteasyAsynchronousContext servletAsyncContext(BooleanSupplier suspended) {
        ResteasyAsynchronousContext context = mock(ResteasyAsynchronousContext.class);
        when(context.isSuspended()).thenAnswer(invocation -> suspended.getAsBoolean());
        when(context.executeBlockingIo(any(), anyBoolean())).thenAnswer(invocation -> {
            CompletableFuture<Void> written = new CompletableFuture<>();
            try {
                invocation.<RunnableWithException> getArgument(0).run();
                written.complete(null);
            } catch (Exception e) {
                written.completeExceptionally(e);
            }
            return written;
        });
        when(context.executeAsyncIo(any())).thenAnswer(invocation -> invocation.getArgument(0));
        return context;
    }

    private AbstractAsynchronousResponse abstractAsynchronousResponse() {
        return mock(AbstractAsynchronousResponse.class,
                withSettings().useConstructor(dispatcher, request, response).defaultAnswer(CALLS_REAL_METHODS));
    }

    private AsyncResponseConsumer consumer() {
        return new AsyncResponseConsumer(null) {
            @Override
            protected boolean sendHeaders() {
                return true;
            }

            @Override
            public void subscribe(Object rtn) {
            }
        };
    }

    private static WebApplicationException forbiddenWithEntity() {
        return new WebApplicationException(
                Response.status(403).entity("forbidden").type(MediaType.TEXT_PLAIN_TYPE).build());
    }

    /**
     * Without a format, every tracing logger shares one event queue, and a flush drains it. An event that is logged
     * after the flush stays in the queue, so the queue must be empty after the write.
     */
    private List<String> traced(Runnable write) {
        unflushedEvents(RESTEasyTracingLogger.create("0", "SUMMARY", "test"));
        RESTEasyTracingLogger logger = RESTEasyTracingLogger.create("1", "SUMMARY", "test");
        request.setAttribute(RESTEasyTracing.PROPERTY_NAME, logger);

        write.run();

        assertEquals(Collections.emptyList(), unflushedEvents(logger), "events logged after the flush");
        return tracingEvents(response.headers());
    }

    private void assertFinishedIsLast(Runnable write) {
        List<String> events = traced(write);
        assertTrue(events.stream().anyMatch(e -> e.contains("WriteTo summary")), () -> "no WI_SUMMARY event in " + events);
        assertTrue(events.stream().anyMatch(e -> e.contains("Response summary")),
                () -> "no RESPONSE_FILTER_SUMMARY event in " + events);
        assertTrue(events.get(events.size() - 1).contains("FINISHED"), () -> "FINISHED is not last in " + events);
    }

    /**
     * @return a completion callback that lets the container recycle the request and the response
     */
    private Consumer<Throwable> recycle() {
        return t -> {
            completions.add(t);
            recycled = true;
        };
    }

    /**
     * @return a completion callback that recycles, then throws as {@code AsyncContext.complete()} can
     */
    private Consumer<Throwable> recycleAndThrow() {
        return t -> {
            completions.add(t);
            if (!recycled) {
                recycled = true;
                throw new IllegalStateException("AsyncContext completed");
            }
        };
    }

    private void recycleAndThrowOnComplete() {
        doAnswer(invocation -> {
            if (!recycled) {
                recycled = true;
                throw new IllegalStateException("AsyncContext completed");
            }
            return null;
        }).when(asyncResponse).complete();
    }

    /**
     * @return the events in the shared queue, which this call drains
     */
    private static List<String> unflushedEvents(RESTEasyTracingLogger logger) {
        // A flush of an empty queue throws, so log one event of our own and leave it out of the result.
        logger.log("FINISHED", 0);
        MultivaluedMap<String, Object> headers = new MultivaluedMapImpl<>();
        logger.flush(headers);
        List<String> events = tracingEvents(headers);
        return events.subList(0, events.size() - 1);
    }

    private static List<String> tracingEvents(MultivaluedMap<String, Object> headers) {
        List<String> events = new ArrayList<>();
        for (int i = 0;; i++) {
            Object event = headers.getFirst(String.format("%s%03d", RESTEasyTracing.HEADER_TRACING_PREFIX, i));
            if (event == null) {
                return events;
            }
            events.add(event.toString());
        }
    }

    /**
     * Captures the late-exception records that {@link AsyncCompletion} logs. The module does not read java.logging, so
     * this class adds that read before it loads its handler.
     */
    private static final class LogCapture {
        static {
            LogCapture.class.getModule().addReads(ModuleLayer.boot().findModule("java.logging").orElseThrow());
        }

        /**
         * @return "ERROR" or "DEBUG" for each late-exception record that {@code action} logs
         */
        static List<String> lateExceptions(Runnable action) {
            return Recorder.record(action);
        }

        private static final class Recorder extends Handler {
            private final List<String> levels = Collections.synchronizedList(new ArrayList<>());

            static List<String> record(Runnable action) {
                java.util.logging.Logger logger = java.util.logging.Logger
                        .getLogger(LogMessages.class.getPackage().getName());
                Recorder recorder = new Recorder();
                Level level = logger.getLevel();
                logger.setLevel(Level.ALL);
                logger.addHandler(recorder);
                try {
                    action.run();
                } finally {
                    logger.removeHandler(recorder);
                    logger.setLevel(level);
                }
                return recorder.levels;
            }

            @Override
            public void publish(LogRecord record) {
                String message = record.getMessage();
                if (message != null && message.contains("Exception after the asynchronous response completed"))
                    levels.add(record.getLevel().intValue() >= Level.SEVERE.intValue() ? "ERROR" : "DEBUG");
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        }
    }

    public static class Pending {
    }

    /**
     * Completes on another thread as soon as the writer callback is chained to it, so the callback runs on that thread
     * before the caller reads the result of the write.
     */
    private static final class CompletesOnAnotherThread extends CompletableFuture<Void> {
        @Override
        public CompletableFuture<Void> whenComplete(BiConsumer<? super Void, ? super Throwable> action) {
            CompletableFuture<Void> dependent = super.whenComplete(action);
            Thread writer = new Thread(() -> complete(null), "async-writer");
            writer.start();
            try {
                writer.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return dependent;
        }
    }

    /**
     * Writes a {@link Pending} asynchronously, and finishes when the test completes the future.
     */
    @Provider
    public static class PendingWriter implements AsyncMessageBodyWriter<Pending> {
        private final CompletableFuture<Void> write;

        PendingWriter(CompletableFuture<Void> write) {
            this.write = write;
        }

        @Override
        public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
            return type == Pending.class;
        }

        @Override
        public void writeTo(Pending pending, Class<?> type, Type genericType, Annotation[] annotations,
                MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) {
        }

        @Override
        public CompletionStage<Void> asyncWriteTo(Pending pending, Class<?> type, Type genericType,
                Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders,
                AsyncOutputStream entityStream) {
            return write;
        }
    }

    private class RecycledResponse extends MockHttpResponse {
        volatile int statusBeforeRecycle;
        volatile boolean resetFails;
        volatile boolean cookieFails;
        volatile boolean asyncOutput;

        @Override
        public OutputStream getOutputStream() throws IOException {
            OutputStream out = super.getOutputStream();
            return asyncOutput ? new BlockingAsyncOutputStream(out) : out;
        }

        /**
         * @return the output headers, without a record of the call
         */
        MultivaluedMap<String, Object> headers() {
            return super.getOutputHeaders();
        }

        @Override
        public void setStatus(int status) {
            if (recycled) {
                callsAfterRecycle.add("setStatus(" + status + ")");
            } else {
                statusBeforeRecycle = status;
            }
            super.setStatus(status);
        }

        @Override
        public MultivaluedMap<String, Object> getOutputHeaders() {
            if (recycled) {
                callsAfterRecycle.add("getOutputHeaders()");
            }
            return super.getOutputHeaders();
        }

        @Override
        public void addNewCookie(NewCookie cookie) {
            if (cookieFails) {
                throw new IllegalArgumentException("bad cookie");
            }
            super.addNewCookie(cookie);
        }

        @Override
        public boolean isCommitted() {
            return !recycled && super.isCommitted();
        }

        @Override
        public void reset() {
            if (recycled) {
                callsAfterRecycle.add("reset()");
            }
            if (resetFails) {
                throw new IllegalStateException("Committed");
            }
            super.reset();
        }

        @Override
        public void sendError(int status) throws IOException {
            if (recycled) {
                callsAfterRecycle.add("sendError(" + status + ")");
            }
            super.sendError(status);
        }
    }
}
