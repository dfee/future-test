package dfee.future;

import io.grpc.Context;
import io.vavr.concurrent.Future;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static dfee.future.Handler.getContextValue;

public class HandlerTest {
  private static final Executor EXECUTOR = Executors.newSingleThreadExecutor();

  /**
   * Sets up a gRPC context with a provided value, and asserts an equivalent return value.
   *
   * @param func returns the thread's context value.
   */
  private static void assertHandler(final Function<UUID, CompletableFuture<UUID>> func) {
    final UUID contextValue = UUID.randomUUID();
    final UUID returnValue;
    try {
      returnValue = Handler.handle(contextValue, func);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    assertThat(returnValue).isEqualTo(contextValue);
  }

  /* Takes a left and (null) right, returning left */
  private static UUID assertRightIsNull(final UUID left, final UUID right) {
    assertThat(right).isNull();
    return left;
  }

  /* Takes a left and right, returning the equal value */
  private static UUID assertRightEqualsLeft(final UUID left, final UUID right) {
    assertThat(left).isEqualTo(right);
    return left;
  }

  /* Asserts the return value of the previous callback equals expected value; returns the value */
  private static Function<UUID, UUID> assertReturnValueEquals(final UUID expected) {
    return (actual) -> assertRightEqualsLeft(expected, actual);
  }

  /* Asserts the return value of the previous callback was null; returns with value */
  private static Function<UUID, UUID> assertReturnValueIsNull(final UUID replacement) {
    return (actual) -> assertRightIsNull(replacement, actual);
  }

  /* Asserts the thread has access to the context; passes through value */
  private static Function<UUID, UUID> assertContextValueIsEqualTo(final UUID expected) {
    return (passThrough) -> {
      assertThat(getContextValue()).isEqualTo(expected);
      return passThrough;
    };
  }

  /* Asserts the thread does not have access to the context; passes through value */
  private static Function<UUID, UUID> assertContextValueIsNull() {
    return (passThrough) -> {
      assertThat(getContextValue()).isNull();
      return passThrough;
    };
  }

  /* Asserts that it's indeterminate whether the thread might will have access to the context; passes through value */
  private static Function<UUID, UUID> assertContextValueIsEqualToIfPresent(final UUID expected) {
    return (passThrough) -> {
      Optional.ofNullable(getContextValue()).ifPresent(v -> assertThat(v).isEqualTo(expected));
      return passThrough;
    };
  }

  /* Ensure expectations with a CompletableFuture.completedFuture */
  @Test
  void testCompletableFuture_CompletedFuture__success() {
    assertHandler(
        (contextValue) ->
            CompletableFuture.completedFuture(getContextValue())
                .thenApply(assertReturnValueEquals(contextValue))
                .thenApply(assertContextValueIsEqualTo(contextValue)));
  }

  /* Ensure expectations with a CompletableFuture.supplyAsync */
  @Test
  void testCompletableFuture_SupplyAsync__failure() {
    assertHandler(
        (contextValue) ->
            CompletableFuture.supplyAsync(Handler::getContextValue)
                .thenApply(assertReturnValueIsNull(contextValue))
                // seems we return to the original thread here
                .thenApply(assertContextValueIsEqualTo(contextValue)));
  }

  @Test
  void testCompletableFuture_SupplyAsync_DirectExecutor__success() {
    assertHandler(
        (contextValue) ->
            CompletableFuture.supplyAsync(Handler::getContextValue, directExecutor())
                .thenApply(assertReturnValueEquals(contextValue))
                .thenApply(assertContextValueIsEqualTo(contextValue)));
  }

  @Test
  void testCompletableFuture_SupplyAsync_ContextExecutor__success() {
    assertHandler(
        (contextValue) ->
            CompletableFuture.supplyAsync(
                    Handler::getContextValue, Context.currentContextExecutor(EXECUTOR))
                .thenApply(assertReturnValueEquals(contextValue))
                .thenApply(assertContextValueIsEqualTo(contextValue)));
  }

  /* Ensure expectations with a Future.successful */
  @Test
  void testFuture_Successful__failure() {
    assertHandler(
        (contextValue) ->
            Future.successful(getContextValue())
                .map(assertReturnValueEquals(contextValue))
                // executing on vavr's DEFAULT_EXECUTOR, so won't have thread context here
                .map(assertContextValueIsNull())
                .toCompletableFuture()
                // seems we won't execute on the original thread here, either
                .thenApply(assertContextValueIsNull()));
  }

  @Test
  void testFuture_Successful_DirectExecutor__success() {
    assertHandler(
        (contextValue) ->
            Future.successful(directExecutor(), getContextValue())
                .map(assertReturnValueEquals(contextValue))
                // executing on *our* direct executor, so we will have thread context here
                .map(assertContextValueIsEqualTo(contextValue))
                .toCompletableFuture()
                .thenApply(assertContextValueIsEqualTo(contextValue)));
  }

  @Test
  void testFuture_Successful_ContextExecutor__success() {
    assertHandler(
        (contextValue) ->
            Future.successful(Context.currentContextExecutor(EXECUTOR), getContextValue())
                .map(assertReturnValueEquals(contextValue))
                // executing on *our* context executor, so we will have thread context here
                .map(assertContextValueIsEqualTo(contextValue))
                .toCompletableFuture()
                .thenApply(assertContextValueIsEqualTo(contextValue)));
  }

  /* Ensure expectations with a Future.fromCompletableFuture */
  @Test
  void testFuture_FromCompletableFuture__failure() {
    assertHandler(
        (contextValue) ->
            Future.fromCompletableFuture(CompletableFuture.completedFuture(getContextValue()))
                .map(assertReturnValueEquals(contextValue))
                // executing on vavr's DEFAULT_EXECUTOR, so won't have thread context here
                .map(assertContextValueIsNull())
                .toCompletableFuture()
                .thenApply(assertContextValueIsNull()));
  }

  @Test
  void testFuture_FromCompletableFuture_DirectExecutor__failure() {
    assertHandler(
        (contextValue) ->
            Future.fromCompletableFuture(
                    directExecutor(), CompletableFuture.completedFuture(getContextValue()))
                .map(assertReturnValueEquals(contextValue))
                // we should be executing on *our* direct executor, but apparently we're not
                .map(assertContextValueIsNull())
                .toCompletableFuture()
                .thenApply(assertContextValueIsNull()));
  }

  @Test
  void testFuture_FromCompletableFuture_ContextExecutor__failure() {
    assertHandler(
        (contextValue) ->
            Future.fromCompletableFuture(
                    Context.currentContextExecutor(EXECUTOR),
                    CompletableFuture.completedFuture(getContextValue()))
                .map(assertReturnValueEquals(contextValue))
                // we should be executing on *our* context executor, but apparently we're not
                .map(assertContextValueIsNull())
                .toCompletableFuture()
                .thenApply(assertContextValueIsNull()));
  }

  /* Ensure expectations with a Future.flatMap Future.successful */
  @Test
  void testFuture_Successful_DirectExecutor_FlatMap_Successful__failure() {
    assertHandler(
        (contextValue) ->
            Future.successful(directExecutor(), getContextValue())
                .map(assertReturnValueEquals(contextValue))
                .map(assertContextValueIsEqualTo(contextValue))
                .flatMap(
                    v ->
                        Future.successful(v)
                            // executing on vavr's DEFAULT_EXECUTOR, so won't have it
                            .map(assertContextValueIsNull()))
                .map(assertReturnValueEquals(contextValue))
                // we *might* get back to the original thread; indeterminate
                .map(assertContextValueIsEqualToIfPresent(contextValue))
                .toCompletableFuture()
                // we *might* get back to the original thread; indeterminate
                .thenApply(assertContextValueIsEqualToIfPresent(contextValue)));
  }

  @Test
  void testFuture_Successful_DirectExecutor_FlatMap_Successful_DirectExecutor__success() {
    assertHandler(
        (contextValue) ->
            Future.successful(directExecutor(), getContextValue())
                .map(assertReturnValueEquals(contextValue))
                .map(assertContextValueIsEqualTo(contextValue))
                .flatMap(
                    v ->
                        Future.successful(directExecutor(), v)
                            // executing on our default executor, so will have it
                            .map(assertContextValueIsEqualTo(contextValue)))
                .flatMap(v -> Future.successful(directExecutor(), v))
                .map(assertReturnValueEquals(contextValue))
                .map(assertContextValueIsEqualTo(contextValue))
                .toCompletableFuture()
                // we *might* get back to the original thread; indeterminate
                .thenApply(assertContextValueIsEqualTo(contextValue)));
  }

  @Test
  void testFuture_Successful_ContextExecutor_FlatMap_Successful__failure() {
    assertHandler(
        (contextValue) ->
            Future.successful(Context.currentContextExecutor(EXECUTOR), getContextValue())
                .map(assertReturnValueEquals(contextValue))
                .map(assertContextValueIsEqualTo(contextValue))
                .flatMap(
                    v ->
                        Future.successful(v)
                            // executing on vavr's DEFAULT_EXECUTOR, so won't have it
                            .map(assertContextValueIsNull()))
                .map(assertReturnValueEquals(contextValue))
                // we *might* get back to the original thread; indeterminate
                .map(assertContextValueIsEqualToIfPresent(contextValue))
                .toCompletableFuture()
                // we *might* get back to the original thread; indeterminate
                .thenApply(assertContextValueIsEqualToIfPresent(contextValue)));
  }

  @Test
  void testFuture_Successful_ContextExecutor_FlatMap_Successful_ContextExecutor__success() {
    assertHandler(
        (contextValue) ->
            Future.successful(Context.currentContextExecutor(EXECUTOR), getContextValue())
                .map(assertReturnValueEquals(contextValue))
                .map(assertContextValueIsEqualTo(contextValue))
                .flatMap(
                    v ->
                        Future.successful(Context.currentContextExecutor(EXECUTOR), v)
                            // executing on our default executor, so will have it
                            .map(assertContextValueIsEqualTo(contextValue)))
                .flatMap(v -> Future.successful(directExecutor(), v))
                .map(assertReturnValueEquals(contextValue))
                .map(assertContextValueIsEqualTo(contextValue))
                .toCompletableFuture()
                // we *might* get back to the original thread; indeterminate
                .thenApply(assertContextValueIsEqualTo(contextValue)));
  }

  /* Ensure expectations with a Future.flatMap Future.fromCompletableFuture CompletableFuture.supplyAsync */
  @Test
  void testFuture_Successful_DirectExecutor_FlatMap_FromCompletableFuture_SupplyAsync__failure() {
    assertHandler(
        (contextValue) ->
            Future.successful(directExecutor(), getContextValue())
                .map(assertReturnValueEquals(contextValue))
                .map(assertContextValueIsEqualTo(contextValue))
                .flatMap(
                    v ->
                        Future.fromCompletableFuture(CompletableFuture.supplyAsync(() -> v))
                            // executing on vavr's DEFAULT_EXECUTOR, so won't have it
                            .map(assertContextValueIsNull()))
                .map(assertReturnValueEquals(contextValue))
                // we *might* get back to the original thread; indeterminate
                .map(assertContextValueIsEqualToIfPresent(contextValue))
                .toCompletableFuture()
                // we *might* get back to the original thread; indeterminate
                .thenApply(assertContextValueIsEqualToIfPresent(contextValue)));
  }

  @Test
  void
      testFuture_Successful_DirectExecutor_FlatMap_FromCompletableFuture_SupplyAsync_DirectExecutor__failure() {
    assertHandler(
        (contextValue) ->
            Future.successful(directExecutor(), getContextValue())
                .map(assertReturnValueEquals(contextValue))
                .map(assertContextValueIsEqualTo(contextValue))
                .flatMap(
                    v ->
                        Future.fromCompletableFuture(
                                CompletableFuture.supplyAsync(() -> v, directExecutor()))
                            // executing on *our* direct executor, expecting to have it, but don't
                            .map(assertContextValueIsNull()))
                .map(assertReturnValueEquals(contextValue))
                // we *might* get back to the original thread; indeterminate
                .map(assertContextValueIsEqualToIfPresent(contextValue))
                .toCompletableFuture()
                // we *might* get back to the original thread; indeterminate
                .thenApply(assertContextValueIsEqualToIfPresent(contextValue)));
  }

  @Test
  void testFuture_Successful_ContextExecutor_FlatMap_FromCompletableFuture_SupplyAsync__failure() {
    assertHandler(
        (contextValue) ->
            Future.successful(Context.currentContextExecutor(EXECUTOR), getContextValue())
                .map(assertReturnValueEquals(contextValue))
                .map(assertContextValueIsEqualTo(contextValue))
                .flatMap(
                    v ->
                        Future.fromCompletableFuture(CompletableFuture.supplyAsync(() -> v))
                            // executing on vavr's DEFAULT_EXECUTOR, so won't have it
                            .map(assertContextValueIsNull()))
                .map(assertReturnValueEquals(contextValue))
                // we *might* get back to the original thread; indeterminate
                .map(assertContextValueIsEqualToIfPresent(contextValue))
                .toCompletableFuture()
                // we *might* get back to the original thread; indeterminate
                .thenApply(assertContextValueIsEqualToIfPresent(contextValue)));
  }

  @Test
  void
      testFuture_Successful_ContextExecutor_FlatMap_FromCompletableFuture_SupplyAsync_ContextExecutor__failure() {
    assertHandler(
        (contextValue) ->
            Future.successful(Context.currentContextExecutor(EXECUTOR), getContextValue())
                .map(assertReturnValueEquals(contextValue))
                .map(assertContextValueIsEqualTo(contextValue))
                .flatMap(
                    v ->
                        Future.fromCompletableFuture(
                                CompletableFuture.supplyAsync(
                                    () -> v, Context.currentContextExecutor(EXECUTOR)))
                            // executing on *our* context executor, expecting to have it, but don't
                            .map(assertContextValueIsNull()))
                .map(assertReturnValueEquals(contextValue))
                // we *might* get back to the original thread; indeterminate
                .map(assertContextValueIsEqualToIfPresent(contextValue))
                .toCompletableFuture()
                // we *might* get back to the original thread; indeterminate
                .thenApply(assertContextValueIsEqualToIfPresent(contextValue)));
  }
}
