import { FeedHandler } from "./base";
import * as feed from "./feed";

const algos: Record<string, FeedHandler> = {
  [feed.shortname]: feed.handler,
};

export default algos;
