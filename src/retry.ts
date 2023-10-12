export async function retry<T>(
  fn: () => Promise<T>,
  opts: {
    maxRetries: number;
    onRetry?: (error: unknown) => void;
    shouldRetry?: (error: unknown) => boolean;
    delay: number;
  }
): Promise<T> {
  let retries = 0;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await fn();
    } catch (error) {
      retries++;
      if (retries >= opts.maxRetries) {
        throw error;
      }
      // rethrow if shouldRetry returns false
      if (opts.shouldRetry && !opts.shouldRetry(error)) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, opts.delay));
      if (opts.onRetry) {
        opts.onRetry(error);
      }
    }
  }
}
