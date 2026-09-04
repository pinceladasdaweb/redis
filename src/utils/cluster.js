// The three questions every collaborator used to answer for itself, each in
// its own words: is this client a cluster, which nodes are its masters, and
// what do we call one of them. Five duck-typed `client.nodes` checks and two
// host:port formatters had grown across index.js, scanner.js and pubsub.js —
// the day one of them honours natMap or brackets an IPv6 address, an operator
// sees the same shard under two names. One place, one answer.

// ioredis exposes `nodes(role)` on Cluster only; a standalone or Sentinel
// client has no such method. Duck-typing it (rather than instanceof) keeps the
// fakes honest: a test client with nodes() IS a cluster to this library.
const isCluster = (client) => typeof client?.nodes === 'function'

// The masters as ioredis's connection pool currently files them — fresh array
// per call. Between a failover and the next slots refresh a promoted replica
// is still under 'slave' here; callers that cannot tolerate that window have
// to refresh first.
const masters = (client) => isCluster(client) ? client.nodes('master') : [client]

// A node's identity, as the pool keys it: the address the driver connected
// to, natMap already applied.
const nodeKey = (node) => {
  const { host, port } = node.options

  return `${host}:${port}`
}

export { isCluster, masters, nodeKey }
