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
