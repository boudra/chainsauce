import { Event } from "@/types";

export class EventQueue {
  #queue: Event[];

  constructor() {
    this.#queue = [];
  }

  enqueue(event: Event): void {
    this.#queue.push(event);
  }

  size(): number {
    return this.#queue.length;
  }

  *drain(): Generator<Event, void, unknown> {
    // sort by block number and log index ascending
    this.#queue.sort((a, b) => {
      if (a.blockNumber < b.blockNumber) {
        return -1;
      }

      if (a.blockNumber > b.blockNumber) {
        return 1;
      }

      if (a.logIndex < b.logIndex) {
        return -1;
      }

      if (a.logIndex > b.logIndex) {
        return 1;
      }

      return 0;
    });

    while (this.#queue.length > 0) {
      const event = this.#queue.shift();
      if (event) {
        yield event;
      }
    }
  }
}
