import { AppContext } from "../config";
import {
  QueryParams,
  OutputSchema as AlgoOutput,
} from "../lexicon/types/app/bsky/feed/getFeedSkeleton";

export abstract class FeedHandler {
  abstract generate(
    ctx: AppContext,
    params: QueryParams,
    requesterDid: string,
  ): Promise<AlgoOutput>;
}
