import {
  OutputSchema,
  QueryParams,
} from "../lexicon/types/app/bsky/feed/getFeedSkeleton";
import { AppContext } from "../config";
import { FeedHandler } from "./base";
import { CreateOp } from "../util/ops";
import { Record as PostRecord } from "../lexicon/types/app/bsky/feed/post";

// max 15 chars
export const shortname = "feed";

export const handler = new (class extends FeedHandler {
  filter(create: CreateOp<PostRecord>): boolean {
    // only alf-related posts
    return create.record.text.toLowerCase().includes("alf");
  }
  async generate(ctx: AppContext, params: QueryParams): Promise<OutputSchema> {
    let builder = ctx.db
      .selectFrom("post")
      .selectAll()
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
