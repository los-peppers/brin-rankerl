-record(node, {
  source,
  degree,
  destinations = []
}).
-type chunk() :: #node{}.
