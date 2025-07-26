import {
  OutputSchema as RepoEvent,
  isCommit,
} from "./lexicon/types/com/atproto/sync/subscribeRepos";
import { getOpsByType } from "./util/ops";
import { FirehoseSubscriptionBase } from "./util/subscription";
import { subDays } from "date-fns";
import { AtpAgent } from "@atproto/api";
import { Config } from "./config";
import { chunk } from "lodash";
import { Database } from "./db";

// ATP record types
type RepoRecord = {
  uri: string;
  cid: string;
  value: Record<string, unknown>;
  repo: string;
  rkey: string;
};

type LikeRecord = {
  subject: {
    uri: string;
    cid: string;
  };
};

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  private agent: AtpAgent;
  private monitoredUsers: Set<string>;
  private readonly BATCH_SIZE = 25;
  private readonly LIKES_PER_USER = 50;
  private readonly TOP_AUTHOR_PERCENTAGE = 0.1;
  private readonly POSTS_PER_AUTHOR = 10;

  constructor(db: Database, service: string) {
    super(db, service);
    this.agent = new AtpAgent({ service });
    // Initialize monitored users set from environment variable
    const monitoredUsersEnv = process.env.FEEDGEN_MONITORED_USERS || "";
    this.monitoredUsers = new Set(
      monitoredUsersEnv
        .split(",")
        .map((did) => did.trim())
        .filter(Boolean),
    );
  }

  private shouldProcessUser(did: string): boolean {
    // If no monitored users are specified, process all users
    return this.monitoredUsers.size === 0 || this.monitoredUsers.has(did);
  }

  private async getLikers(uri: string): Promise<string[]> {
    try {
      const did = uri.split("/")[2];
      const likes = await this.agent.api.com.atproto.repo.listRecords({
        collection: "app.bsky.feed.like",
        repo: did,
        limit: 100,
      });

      const records = likes.data.records as RepoRecord[];
      return records
        .filter((record) => {
          const like = record.value as LikeRecord;
          return like.subject.uri === uri;
        })
        .map((record) => record.repo);
    } catch (err) {
      console.warn(`Failed to get likes for post ${uri}:`, err);
      return [];
    }
  }

  private async getUserLikes(
    did: string,
  ): Promise<Array<{ authorDid: string; postUri: string; postCid: string }>> {
    try {
      const likes = await this.agent.api.com.atproto.repo.listRecords({
        collection: "app.bsky.feed.like",
        repo: did,
        limit: this.LIKES_PER_USER,
      });

      const records = likes.data.records as RepoRecord[];
      return records.map((record) => {
        const like = record.value as LikeRecord;
        return {
          authorDid: like.subject.uri.split("/")[2],
          postUri: like.subject.uri,
          postCid: like.subject.cid,
        };
      });
    } catch (err) {
      console.warn(`Failed to get likes for user ${did}:`, err);
      return [];
    }
  }

  private async getAuthorPosts(
    did: string,
  ): Promise<Array<{ uri: string; cid: string }>> {
    try {
      const posts = await this.agent.api.com.atproto.repo.listRecords({
        collection: "app.bsky.feed.post",
        repo: did,
        limit: this.POSTS_PER_AUTHOR,
      });

      const records = posts.data.records as RepoRecord[];
      return records.map((record) => ({
        uri: `at://${did}/app.bsky.feed.post/${record.rkey}`,
        cid: record.cid,
      }));
    } catch (err) {
      console.warn(`Failed to get posts for author ${did}:`, err);
      return [];
    }
  }

  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return;

    const ops = await getOpsByType(evt);

    const postsToDelete = ops.posts.deletes.map((del) => del.uri);
    const postsToCreate: Array<{
      id: string;
      uri: string;
      cid: string;
      viaLiker: string;
      indexedAt: string;
    }> = [];

    // Process new likes in batches to avoid rate limiting
    const likeBatches = chunk(ops.likes.creates, this.BATCH_SIZE);
    for (const batch of likeBatches) {
      await Promise.all(
        batch.map(async (like) => {
          if (!this.shouldProcessUser(like.author)) return;

          try {
            // 1. Get all users who liked the same post
            const coLikers = await this.getLikers(like.record.subject.uri);

            // 2. Build author histogram from co-likers' recent activity
            const authorHistogram = new Map<string, number>();
            const coLikerLikes = await Promise.all(
              coLikers.map((did) => this.getUserLikes(did)),
            );

            // Flatten and count author occurrences
            coLikerLikes.flat().forEach(({ authorDid }) => {
              authorHistogram.set(
                authorDid,
                (authorHistogram.get(authorDid) ?? 0) + 1,
              );
            });

            // 3. Select top authors based on like frequency
            const topAuthors = [...authorHistogram.entries()]
              .sort((a, b) => b[1] - a[1])
              .slice(
                0,
                Math.max(
                  1,
                  Math.ceil(authorHistogram.size * this.TOP_AUTHOR_PERCENTAGE),
                ),
              )
              .map(([did]) => did);

            // 4. Get recent posts from top authors
            const authorPosts = await Promise.all(
              topAuthors.map((did) => this.getAuthorPosts(did)),
            );

            // 5. Add posts to feed
            authorPosts.flat().forEach((post) => {
              postsToCreate.push({
                id: `${post.uri}#${like.author}`,
                uri: post.uri,
                cid: post.cid,
                viaLiker: like.author,
                indexedAt: new Date().toISOString(),
              });
            });
          } catch (err) {
            console.error("Error processing like for feed generation:", err);
          }
        }),
      );

      // Add a small delay between batches to respect rate limits
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    const deadline = subDays(new Date(), 2);
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
