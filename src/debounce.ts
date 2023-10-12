export default function debounce<F extends (...args: unknown[]) => unknown>(
  func: F,
  wait: number
) {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  const executedFunction = (...args: Parameters<F>) => {
    const later = function () {
      timeout = undefined;
      func(...args);
    };

    clearTimeout(timeout);

    timeout = setTimeout(later, wait);
  };

  executedFunction.cancel = function () {
    clearTimeout(timeout);
    timeout = undefined;
  };

  return executedFunction;
}
