import type { Storage, Subscription } from "../index";
import { ethers } from "ethers";

export default class PrismaStorage<T> implements Storage {
  db: T | any;

  constructor(db: T) {
    this.db = db;
  }

  async getSubscriptions(): Promise<Subscription[]> {
    let subscriptions = await this.db.subscription.findMany();

    return subscriptions.map((sub: any) => ({
      address: sub.address,
      contract: new ethers.Contract(sub.address, JSON.parse(sub.abi)),
      fromBlock: sub.fromBlock,
    }));
  }

  async setSubscriptions(subscriptions: Subscription[]): Promise<void> {
    let subs = subscriptions.map((sub) => ({
      address: sub.address,
      abi: sub.contract.interface.format(
        ethers.utils.FormatTypes.json
      ) as string,
      fromBlock: sub.fromBlock,
    }));

    this.db.$transaction([
      this.db.subscription.deleteMany(),
      this.db.subscription.createMany(subs),
    ]);
  }
}
