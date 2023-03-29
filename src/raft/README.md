# Lab 2A

We mainly need to implement Leader Election and HeartBeats.

## Task:

**Leader Election**

If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate.

On conversion to candidate, start election:

* Increment currentTerm
* Vote for self
* Reset election timer
* Send RequestVote RPCs to all other servers

If votes received from majority of servers: become leader.

If AppendEntries RPC received from new leader: convert to follower.

If election timeout elapses: start new election.

**Heartbeat**

Leader periodically send AppendEntries RPC to other servers with an empty log entries.

The server who receive AppendEntries RPC should reset its election timeout.

## Details:

There are several mechanics achieving the time ticker.

1. Use time.Ticker.
4. Use values to store the last heartbeat or vote time, and initiate a go routine to check this values every election timeout.
5. Use channel as a signal to indicate next execution.

If you choose to use channel, there is one thing you need to take seriously care about it:

**Using channel in a mutex may block execution if it need this mutex to receive this value!**

So there is a more proper way to send the value to channel, use select-case-default to filer the unreceivable values.


Choose a suitable election timeout, for example, if you choose 150ms as heartbeat timeout, so 

election timeout = 250ms(base time >> 150ms) + rand time(large enough to avoid election at the same time)


While receiving RequestVote from candidate, remember to set voteFor = null if term > currentTerm before voting.

## References:

1. Diego Ongaro and John Ousterhout (2014). "In Search of an Understandable Consensus Algorithm (Extended Version)." Technical Report No. 183, Stanford University. Available online: [https://raft.github.io/raft.pdf](https://raft.github.io/raft.pdf).
