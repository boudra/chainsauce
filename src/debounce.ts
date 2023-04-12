type Debounced<F extends (...args: unknown[]) => unknown> = F & {
  cancel: () => void;
  isScheduled: () => boolean;
  now: (...args: Parameters<F>) => void;
};

export default function debounce<F extends (...args: unknown[]) => unknown>(
  func: F,
  wait: number
): Debounced<F> {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  const debounced = function (...args: Parameters<F>) {
    const later = () => {
      timeout = undefined;
      func(...args);
    };

    clearTimeout(timeout);

    timeout = setTimeout(later, wait);
  } as Debounced<F>;

  debounced.cancel = () => {
    clearTimeout(timeout);
  };

  debounced.now = (...args: Parameters<F>) => {
    clearTimeout(timeout);
    func(...args);
  };

  debounced.isScheduled = () => {
    return timeout !== undefined;
  };

  return debounced;
}
