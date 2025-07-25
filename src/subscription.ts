import {
  OutputSchema as RepoEvent,
  isCommit,
} from "./lexicon/types/com/atproto/sync/subscribeRepos";
import { getOpsByType } from "./util/ops";
import { FirehoseSubscriptionBase } from "./util/subscription";
import algos from "./algos";
import { subMinutes } from "date-fns";

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return;

    const ops = await getOpsByType(evt);

    const postsToDelete = ops.posts.deletes.map((del) => del.uri);
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        return Object.values(algos).some((algo) => algo.filter(create));
      })
      .map((create) => {
        // map to db row
        return {
          uri: create.uri,
          cid: create.cid,
          indexedAt: new Date().toISOString(),
        };
      });

    // delete posts that are either in the delete list or older than 15 minutes
    // this is to prevent the db from growing indefinitely
    const deadline = subMinutes(new Date(), 15);
    await this.db
      .deleteFrom("post")
      .where((eb) =>
        eb.or([
          eb("uri", "in", postsToDelete),
          eb("indexedAt", "<", deadline.toISOString()),
        ]),
      )
      .execute();

    if (postsToCreate.length > 0) {
      await this.db
        .insertInto("post")
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute();
    }
  }
}
