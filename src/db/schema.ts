export type DatabaseSchema = {
  post: Post;
  sub_state: SubState;
};

export type Post = {
  id: string; // URI and liker DID
  uri: string;
  cid: string;
  viaLiker: string; // The DID of the user who liked the post, used for feed generation
  indexedAt: string;
};

export type SubState = {
  service: string;
  cursor: number;
};
