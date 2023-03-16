export default function debounce<F extends (...args: unknown[]) => unknown>(
  func: F,
  wait: number,
  immediate: boolean
) {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  return function executedFunction(...args: Parameters<F>) {
    const later = function () {
      timeout = undefined;
      if (!immediate) func(...args);
    };

    const callNow = immediate && !timeout;

    clearTimeout(timeout);

    timeout = setTimeout(later, wait);

    if (callNow) {
      func(...args);
    }
  };
}
