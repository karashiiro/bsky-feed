import {
  OutputSchema as RepoEvent,
  isCommit,
} from "./lexicon/types/com/atproto/sync/subscribeRepos";
import { getOpsByType, isLike, isPost } from "./util/ops";
import { FirehoseSubscriptionBase } from "./util/subscription";
import { subDays } from "date-fns";
import { AtpAgent } from "@atproto/api";
import { chunk } from "lodash";
import { Database } from "./db";

interface PDSDirectoryResponse {
  service: {
    serviceEndpoint: string;
  }[];
}

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  private agent: AtpAgent;
  private monitoredUsers: Set<string>;
  private readonly BATCH_SIZE = 25;
  private readonly LIKES_PER_USER = 50;
  private readonly TOP_AUTHOR_PERCENTAGE = 0.1;
  private readonly POSTS_PER_AUTHOR = 10;

  constructor(db: Database, service: string) {
    super(db, service);
    this.agent = new AtpAgent({ service: "https://bsky.social" });
    // Initialize monitored users set from environment variable
    const monitoredUsersEnv = process.env.FEEDGEN_MONITORED_USERS || "";
    this.monitoredUsers = new Set(
      monitoredUsersEnv
        .split(",")
        .map((did) => did.trim())
        .filter(Boolean),
    );
  }

  private async ensureAuth() {
    if (
      !process.env.FEEDGEN_ATP_IDENTIFIER ||
      !process.env.FEEDGEN_ATP_APP_PASSWORD
    ) {
      throw new Error(
        "ATP credentials not configured. Please set FEEDGEN_ATP_IDENTIFIER and FEEDGEN_ATP_APP_PASSWORD",
      );
    }

    try {
      await this.agent.login({
        identifier: process.env.FEEDGEN_ATP_IDENTIFIER,
        password: process.env.FEEDGEN_ATP_APP_PASSWORD,
      });
    } catch (err) {
      console.error("Failed to authenticate with ATP:", err);
      throw err;
    }
  }

  private shouldProcessUser(did: string): boolean {
    // If no monitored users are specified, process all users
    return this.monitoredUsers.size === 0 || this.monitoredUsers.has(did);
  }

  private async getServiceForIdentity(did: string): Promise<string> {
    try {
      const profile = await fetch(`https://plc.directory/${did}/`);
      const data = (await profile.json()) as PDSDirectoryResponse;
      const serviceEndpoint = data.service[0]?.serviceEndpoint;
      if (!serviceEndpoint) {
        throw new Error(`No service endpoint found for identity ${did}`);
      }
      return serviceEndpoint;
    } catch (err) {
      console.warn(`Failed to get service for identity ${did}:`, err);
      throw err;
    }
  }

  private async getLikers(uri: string): Promise<string[]> {
    await this.ensureAuth();
    try {
      // First, get the liked post
      const [did, collection, rkey] = uri.split("/").slice(2);
      const post = await this.agent.com.atproto.repo.getRecord({
        collection,
        repo: did,
        rkey,
      });

      // Then get the likes
      const likes = await this.agent.app.bsky.feed.getLikes({
        uri,
        cid: post.data.cid,
        limit: 100,
      });

      return likes.data.likes
        .map((like) => like.actor.did)
        .filter((did) => !did.startsWith("did:web:"));
    } catch (err) {
      console.warn(`Failed to get likes for post ${uri}:`, err);
      return [];
    }
  }

  private async getUserLikedPosts(did: string) {
    try {
      const serviceAgent = new AtpAgent({
        service: await this.getServiceForIdentity(did),
      });
      const likes = await serviceAgent.com.atproto.repo.listRecords({
        collection: "app.bsky.feed.like",
        limit: this.LIKES_PER_USER,
        repo: did,
      });

      return likes.data.records.map((item) => item.value).filter(isLike);
    } catch (err) {
      console.warn(`Failed to get likes for user ${did}:`, err);
      throw err;
    }
  }

  private getPostAuthor(postUri: string) {
    try {
      const [did, ,] = postUri.split("/").slice(2);
      if (!did) {
        throw new Error(`Invalid post URI: ${postUri}`);
      }

      return did;
    } catch (err) {
      console.warn(`Failed to get author for post ${postUri}:`, err);
      throw err;
    }
  }

  private async getAuthorPosts(did: string) {
    try {
      const serviceAgent = new AtpAgent({
        service: await this.getServiceForIdentity(did),
      });
      const posts = await serviceAgent.com.atproto.repo.listRecords({
        collection: "app.bsky.feed.post",
        limit: this.POSTS_PER_AUTHOR,
        repo: did,
      });

      return posts.data.records;
    } catch (err) {
      console.warn(`Failed to get posts for author ${did}:`, err);
      throw err;
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
              coLikers.map((did) => this.getUserLikedPosts(did)),
            );

            // Flatten and count author occurrences
            coLikerLikes.flat().forEach(({ subject: { uri } }) => {
              const authorDid = this.getPostAuthor(uri);
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
