import { Subscription } from "@atproto/xrpc-server";
import { ids, lexicons } from "../lexicon/lexicons";

import {
  OutputSchema as RepoEvent,
  isCommit,
} from "../lexicon/types/com/atproto/sync/subscribeRepos";
import { Database } from "../db";

export abstract class FirehoseSubscriptionBase {
  public sub: Subscription<RepoEvent>;

  constructor(
    public db: Database,
    public service: string,
  ) {
    this.sub = new Subscription({
      service: service,
      method: ids.ComAtprotoSyncSubscribeRepos,
      getParams: () => this.getCursor(),
      validate: (value: unknown) => {
        try {
          return lexicons.assertValidXrpcMessage<RepoEvent>(
            ids.ComAtprotoSyncSubscribeRepos,
            value,
          );
        } catch (err) {
          console.error("repo subscription skipped invalid message", err);
        }
      },
    });
  }

  abstract handleEvent(evt: RepoEvent): Promise<void>;

  async run(subscriptionReconnectDelay: number) {
    try {
      for await (const evt of this.sub) {
        this.handleEvent(evt).catch((err) => {
          console.error("repo subscription could not handle message", err);
        });
        // update stored cursor every 20 events or so
        if (isCommit(evt) && evt.seq % 20 === 0) {
          await this.updateCursor(evt.seq);
        }
      }
    } catch (err) {
      console.error("repo subscription errored", err);
      setTimeout(
        () => this.run(subscriptionReconnectDelay),
        subscriptionReconnectDelay,
      );
    }
  }

  async updateCursor(cursor: number) {
    await this.db
      .updateTable("sub_state")
      .set({ cursor })
      .where("service", "=", this.service)
      .execute();
  }

  async getCursor(): Promise<{ cursor?: number }> {
    const res = await this.db
      .selectFrom("sub_state")
      .selectAll()
      .where("service", "=", this.service)
      .executeTakeFirst();
    return res ? { cursor: res.cursor } : {};
  }
}
