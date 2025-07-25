import { AppContext } from "../config";
import {
  QueryParams,
  OutputSchema as AlgoOutput,
} from "../lexicon/types/app/bsky/feed/getFeedSkeleton";
import { CreateOp } from "../util/ops";
import { Record as PostRecord } from "../lexicon/types/app/bsky/feed/post";

export abstract class FeedHandler {
  abstract filter(create: CreateOp<PostRecord>): boolean;
  abstract generate(ctx: AppContext, params: QueryParams): Promise<AlgoOutput>;
}
