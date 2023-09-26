import { retry } from "./retry";
import { describe, test, expect, vi } from "vitest";

describe("retry function", () => {
  test("should succeed on first try", async () => {
    const fn = vi.fn().mockResolvedValue("success");
    const onRetry = vi.fn();

    const result = await retry(fn, {
      maxRetries: 3,
      onRetry,
      delay: 100,
    });

    expect(result).toBe("success");
    expect(onRetry).not.toHaveBeenCalled();
  });

  test("should succeed after one retry", async () => {
    const fn = vi
      .fn()
      .mockRejectedValueOnce(new Error("fail"))
      .mockResolvedValue("success");
    const onRetry = vi.fn();

    const result = await retry(fn, {
      maxRetries: 3,
      onRetry,
      delay: 100,
    });

    expect(result).toBe("success");
    expect(onRetry).toHaveBeenCalledTimes(1);
  });

  test("should fail with RetryError after maxRetries", async () => {
    const fn = vi.fn().mockRejectedValue(new Error("fail"));
    const onRetry = vi.fn();

    await expect(
      retry(fn, {
        maxRetries: 3,
        onRetry,
        delay: 100,
      })
    ).rejects.toThrow(Error);

    expect(onRetry).toHaveBeenCalledTimes(2);
  });

  test("should not retry if shouldRetry returns false", async () => {
    const fn = vi.fn().mockRejectedValue(new Error("fail"));
    const onRetry = vi.fn();
    const shouldRetry = vi.fn().mockReturnValue(false);

    await expect(
      retry(fn, {
        maxRetries: 3,
        onRetry,
        shouldRetry,
        delay: 100,
      })
    ).rejects.toThrow(new Error("fail"));

    expect(onRetry).not.toHaveBeenCalled();
    expect(shouldRetry).toHaveBeenCalledTimes(1);
  });
});
