import { FeedHandler } from "./base";
import * as whatsAlf from "./whats-alf";

const algos: Record<string, FeedHandler> = {
  [whatsAlf.shortname]: whatsAlf.handler,
};

export default algos;
