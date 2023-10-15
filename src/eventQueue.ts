import { Event } from "@/types";

export interface EventQueue {
  queue(event: Event): void;
  size(): number;
  drain(): Generator<Event, void, unknown>;
}

// TODO: priority queue
export function createEventQueue() {
  const queue: Event[] = [];

  return {
    queue(event: Event) {
      queue.push(event);
    },
    size() {
      return queue.length;
    },
    *drain() {
      // sort by block number and log index ascending
      queue.sort((a, b) => {
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

      while (queue.length > 0) {
        const event = queue.shift();
        if (event) {
          yield event;
        }
      }
    },
  };
}
