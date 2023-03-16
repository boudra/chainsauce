export default function debounce<F extends (...args: unknown[]) => unknown>(
  func: F,
  wait: number
) {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  return function executedFunction(...args: Parameters<F>) {
    const later = function () {
      timeout = undefined;
      func(...args);
    };

    clearTimeout(timeout);

    timeout = setTimeout(later, wait);
  };
}
