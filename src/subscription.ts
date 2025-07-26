import {
  OutputSchema as RepoEvent,
  isCommit,
} from "./lexicon/types/com/atproto/sync/subscribeRepos";
import { getOpsByType } from "./util/ops";
import { FirehoseSubscriptionBase } from "./util/subscription";
import { subMinutes } from "date-fns";

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return;

    const ops = await getOpsByType(evt);

    const postsToDelete = ops.posts.deletes.map((del) => del.uri);
    const postsToCreate = [];

    // TODO: The whole graph algorithm, outputs go in postsToCreate
    const likes = ops.likes.creates;

    /* pseudocode for like graph algorithm
    when like created:
      // identifying cluster via sampling
      create a histogram
      get users for all likes of the liked post
      for each user:
        get their latest N likes
        add authors of those posts to histogram
      select top P% users from histogram
      // post selection from cluster
      select latest M posts from each
      shuffle those into the feed for the user who liked the original post
     */

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
