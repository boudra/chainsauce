import { ListenerSignature, TypedEmitter } from "tiny-typed-emitter";

export class AsyncEventEmitter<
  L extends ListenerSignature<L>
> extends TypedEmitter<L> {
  async emitAsync<U extends keyof L>(
    event: keyof L,
    ...args: Parameters<L[U]>
  ) {
    const listeners = this.listeners(event);
    const promises = listeners.map((listener) => listener(...args));
    await Promise.all(promises);
  }
}
