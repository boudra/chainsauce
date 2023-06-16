/**
 * Returns a debounced version of the given function that delays its execution until a certain amount of time has passed since the last time it was called.
 * @param {(...args: unknown[]) => unknown} func - The function to debounce.
 * @param {number} wait - The number of milliseconds to wait before executing the debounced function.
 * @returns A debounced version of the given function.
 */
export default function debounce<F extends (...args: unknown[]) => unknown>(
  func: F,
  wait: number
) {
  /**
   * The timeout ID returned by `setTimeout`.
   * @type {ReturnType<typeof setTimeout> | undefined}
   */
  let timeout: ReturnType<typeof setTimeout> | undefined;

  /**
   * The debounced function that will be returned.
   * @param {...Parameters<F>} args - The arguments to pass to the original function.
   */
  return function executedFunction(...args: Parameters<F>) {
    /**
     * The function to execute after the debounce time has passed.
     */
    const later = function () {
      timeout = undefined;
      func(...args);
    };

    /**
     * Clears the previous timeout and sets a new one.
     */
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}
