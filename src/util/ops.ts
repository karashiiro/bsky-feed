import { Record as PostRecord } from "../lexicon/types/app/bsky/feed/post";
import { Record as RepostRecord } from "../lexicon/types/app/bsky/feed/repost";
import { Record as LikeRecord } from "../lexicon/types/app/bsky/feed/like";
import { Record as FollowRecord } from "../lexicon/types/app/bsky/graph/follow";
import { BlobRef } from "@atproto/lexicon";
import { ids, lexicons } from "../lexicon/lexicons";
import { cborToLexRecord, readCar } from "@atproto/repo";
import { Commit } from "../lexicon/types/com/atproto/sync/subscribeRepos";

export const getOpsByType = async (evt: Commit): Promise<OperationsByType> => {
  const car = await readCar(evt.blocks);
  const opsByType: OperationsByType = {
    posts: { creates: [], deletes: [] },
    reposts: { creates: [], deletes: [] },
    likes: { creates: [], deletes: [] },
    follows: { creates: [], deletes: [] },
  };

  for (const op of evt.ops) {
    const uri = `at://${evt.repo}/${op.path}`;
    const [collection] = op.path.split("/");

    if (op.action === "update") continue; // updates not supported yet

    if (op.action === "create") {
      if (!op.cid) continue;
      const recordBytes = car.blocks.get(op.cid);
      if (!recordBytes) continue;
      const record = cborToLexRecord(recordBytes);
      const create = { uri, cid: op.cid.toString(), author: evt.repo };
      if (collection === ids.AppBskyFeedPost && isPost(record)) {
        opsByType.posts.creates.push({ record, ...create });
      } else if (collection === ids.AppBskyFeedRepost && isRepost(record)) {
        opsByType.reposts.creates.push({ record, ...create });
      } else if (collection === ids.AppBskyFeedLike && isLike(record)) {
        opsByType.likes.creates.push({ record, ...create });
      } else if (collection === ids.AppBskyGraphFollow && isFollow(record)) {
        opsByType.follows.creates.push({ record, ...create });
      }
    }

    if (op.action === "delete") {
      if (collection === ids.AppBskyFeedPost) {
        opsByType.posts.deletes.push({ uri });
      } else if (collection === ids.AppBskyFeedRepost) {
        opsByType.reposts.deletes.push({ uri });
      } else if (collection === ids.AppBskyFeedLike) {
        opsByType.likes.deletes.push({ uri });
      } else if (collection === ids.AppBskyGraphFollow) {
        opsByType.follows.deletes.push({ uri });
      }
    }
  }

  return opsByType;
};

export type OperationsByType = {
  posts: Operations<PostRecord>;
  reposts: Operations<RepostRecord>;
  likes: Operations<LikeRecord>;
  follows: Operations<FollowRecord>;
};

export type Operations<T = Record<string, unknown>> = {
  creates: CreateOp<T>[];
  deletes: DeleteOp[];
};

export type CreateOp<T> = {
  uri: string;
  cid: string;
  author: string;
  record: T;
};

export type DeleteOp = {
  uri: string;
};

export const isPost = (obj: unknown): obj is PostRecord => {
  return isType(obj, ids.AppBskyFeedPost);
};

export const isRepost = (obj: unknown): obj is RepostRecord => {
  return isType(obj, ids.AppBskyFeedRepost);
};

export const isLike = (obj: unknown): obj is LikeRecord => {
  return isType(obj, ids.AppBskyFeedLike);
};

export const isFollow = (obj: unknown): obj is FollowRecord => {
  return isType(obj, ids.AppBskyGraphFollow);
};

const isType = (obj: unknown, nsid: string) => {
  try {
    lexicons.assertValidRecord(nsid, fixBlobRefs(obj));
    return true;
  } catch (err) {
    return false;
  }
};

// @TODO right now record validation fails on BlobRefs
// simply because multiple packages have their own copy
// of the BlobRef class, causing instanceof checks to fail.
// This is a temporary solution.
const fixBlobRefs = (obj: unknown): unknown => {
  if (Array.isArray(obj)) {
    return obj.map(fixBlobRefs);
  }
  if (obj && typeof obj === "object") {
    if (obj.constructor.name === "BlobRef") {
      const blob = obj as BlobRef;
      return new BlobRef(blob.ref, blob.mimeType, blob.size, blob.original);
    }
    return Object.entries(obj).reduce(
      (acc, [key, val]) => {
        return Object.assign(acc, { [key]: fixBlobRefs(val) });
      },
      {} as Record<string, unknown>,
    );
  }
  return obj;
};
