package dfee.future;

import io.grpc.Context;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class Handler {
  private static final Context.Key<UUID> CONTEXT_KEY = Context.key("key");

  public static UUID getContextValue() {
    return CONTEXT_KEY.get();
  }

  public static UUID handle(
      final UUID contextValue, final Function<UUID, CompletableFuture<UUID>> executor)
      throws Exception {
    return Context.current()
        .withValue(CONTEXT_KEY, contextValue)
        .wrap(() -> executor.apply(contextValue))
        .call()
        .join();
  }
}
