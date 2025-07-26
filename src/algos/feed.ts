import {
  OutputSchema,
  QueryParams,
} from "../lexicon/types/app/bsky/feed/getFeedSkeleton";
import { AppContext } from "../config";
import { FeedHandler } from "./base";

// max 15 chars
export const shortname = "feed";

export const handler = new (class extends FeedHandler {
  async generate(
    ctx: AppContext,
    params: QueryParams,
    requesterDid: string,
  ): Promise<OutputSchema> {
    let builder = ctx.db
      .selectFrom("post")
      .selectAll()
      .where("viaLiker", "=", requesterDid)
      .orderBy("indexedAt", "desc")
      .orderBy("cid", "desc")
      .limit(params.limit);

    if (params.cursor) {
      const timeStr = new Date(parseInt(params.cursor, 10)).toISOString();
      builder = builder.where("post.indexedAt", "<", timeStr);
    }
    const res = await builder.execute();

    const feed = res.map((row) => ({
      post: row.uri,
    }));

    let cursor: string | undefined;
    const last = res.at(-1);
    if (last) {
      cursor = new Date(last.indexedAt).getTime().toString(10);
    }

    return {
      cursor,
      feed,
    };
  }
})();
