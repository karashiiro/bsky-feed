import { InvalidRequestError } from "@atproto/xrpc-server";
import { Server } from "../lexicon";
import { AppContext } from "../config";
import algos from "../algos";
import { AtUri } from "@atproto/syntax";
import { validateAuth } from "../auth";

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.getFeedSkeleton(async ({ params, req }) => {
    const feedUri = new AtUri(params.feed);
    const algo = algos[feedUri.rkey];
    if (
      feedUri.hostname !== ctx.cfg.publisherDid ||
      feedUri.collection !== "app.bsky.feed.generator" ||
      !algo
    ) {
      throw new InvalidRequestError(
        "Unsupported algorithm",
        "UnsupportedAlgorithm",
      );
    }

    const requesterDid = await validateAuth(
      req,
      ctx.cfg.serviceDid,
      ctx.didResolver,
    );

    const body = await algo.generate(ctx, params, requesterDid);
    return {
      encoding: "application/json",
      body: body,
    };
  });
}
