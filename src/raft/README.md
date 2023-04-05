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
2. Use values to store the last heartbeat or vote time, and initiate a go routine to check this values every election timeout.
3. Use channel as a signal to indicate next execution.

If you choose to use channel, there is one thing you need to take seriously care about it:

**Using channel in a mutex may block execution if it need this mutex to receive this value!**

So there is a more proper way to send the value to channel, use select-case-default to filer the unreceivable values.

Choose a suitable election timeout, for example, if you choose 150ms as heartbeat timeout, so

election timeout = 250ms(base time >> 150ms) + rand time(large enough to avoid election at the same time)

While receiving RequestVote from candidate, remember to set voteFor = null if term > currentTerm before voting.

# Lab 2B

## Task:

**AppendEntries**

1. Find the match index by prevLogIndex and prevLogItem.
2. Replace the entries with given leader entries behind the match index.
3. Update leader nextIndex and matchIndex.

**Election restriction:**

vote yes if last term <= candidate term, or same term but len(log) <= len(clog)

## Result and Bottlenect:

```bash
Test (2B): basic agreement ...
  ... Passed --   1.1  3   16    4282    3
Test (2B): RPC byte count ...
  ... Passed --   2.6  3   48  113574   11
Test (2B): test progressive failure of followers ...
  ... Passed --   4.8  3  118   25248    3
Test (2B): test failure of leaders ...
  ... Passed --   5.2  3  186   41720    3
Test (2B): agreement after follower reconnects ...
  ... Passed --   6.1  3  122   31495    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.8  5  196   40528    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.9  3   12    3222    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   4.6  3  150   35338    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  26.9  5 2220 1686015  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.4  3   44   12270   12
PASS
ok  	6.5840/raft	58.605s
```

Bottlenext occurs at Test (2B): leader backs up quickly over incorrect follower logs, and my part of debug logs:

```
2023/04/01 13:32:04 2 reply true and 1, update log length: 0
2023/04/01 13:32:04 Start agreement: index 2
2023/04/01 13:32:04 Start agreement: index 3
2023/04/01 13:32:04 Start agreement: index 4
2023/04/01 13:32:04 Start agreement: index 5
2023/04/01 13:32:04 Start agreement: index 6
2023/04/01 13:32:04 Start agreement: index 7
2023/04/01 13:32:04 Start agreement: index 8
2023/04/01 13:32:04 Start agreement: index 9
2023/04/01 13:32:04 Start agreement: index 10
2023/04/01 13:32:04 Start agreement: index 11
2023/04/01 13:32:04 Start agreement: index 12
2023/04/01 13:32:04 Start agreement: index 13
2023/04/01 13:32:04 Start agreement: index 14
2023/04/01 13:32:04 Start agreement: index 15
2023/04/01 13:32:04 Start agreement: index 16
2023/04/01 13:32:04 Start agreement: index 17
2023/04/01 13:32:04 Start agreement: index 18
2023/04/01 13:32:04 Start agreement: index 19
2023/04/01 13:32:04 Start agreement: index 20
2023/04/01 13:32:04 Start agreement: index 21
2023/04/01 13:32:04 Start agreement: index 22
2023/04/01 13:32:04 Start agreement: index 23
2023/04/01 13:32:04 Start agreement: index 24
2023/04/01 13:32:04 Start agreement: index 25
2023/04/01 13:32:04 Start agreement: index 26
2023/04/01 13:32:04 Start agreement: index 27
2023/04/01 13:32:04 Start agreement: index 28
2023/04/01 13:32:04 Start agreement: index 29
2023/04/01 13:32:04 Start agreement: index 30
2023/04/01 13:32:04 Start agreement: index 31
2023/04/01 13:32:04 Start agreement: index 32
2023/04/01 13:32:04 Start agreement: index 33
2023/04/01 13:32:04 Start agreement: index 34
2023/04/01 13:32:04 Start agreement: index 35
2023/04/01 13:32:04 Start agreement: index 36
2023/04/01 13:32:04 Start agreement: index 37
2023/04/01 13:32:04 Start agreement: index 38
2023/04/01 13:32:04 Start agreement: index 39
2023/04/01 13:32:04 Start agreement: index 40
2023/04/01 13:32:04 Start agreement: index 41
2023/04/01 13:32:04 Start agreement: index 42
2023/04/01 13:32:04 Start agreement: index 43
2023/04/01 13:32:04 Start agreement: index 44
2023/04/01 13:32:04 Start agreement: index 45
2023/04/01 13:32:04 Start agreement: index 46
2023/04/01 13:32:04 Start agreement: index 47
2023/04/01 13:32:04 Start agreement: index 48
2023/04/01 13:32:04 Start agreement: index 49
2023/04/01 13:32:04 Start agreement: index 50
2023/04/01 13:32:04 Start agreement: index 51
2023/04/01 13:32:04 3 sendAppendEntries to 4 with pivot(1, 1) log: 50
2023/04/01 13:32:04 3 sendAppendEntries to 1 with pivot(1, 1) log: 50
2023/04/01 13:32:04 3 sendAppendEntries to 0 with pivot(1, 1) log: 50
2023/04/01 13:32:04 3 sendAppendEntries to 2 with pivot(1, 1) log: 50
2023/04/01 13:32:04 4 reply true and 1, update log length: 50
...
Start agreement: index 2
2023/04/01 13:32:05 3 sendAppendEntries to 4 with pivot(51, 1) log: 0
2023/04/01 13:32:05 3 sendAppendEntries to 0 with pivot(1, 1) log: 50
2023/04/01 13:32:05 3 sendAppendEntries to 1 with pivot(1, 1) log: 50
2023/04/01 13:32:05 3 sendAppendEntries to 2 with pivot(1, 1) log: 50
2023/04/01 13:32:05 0 sendAppendEntries to 4 with pivot(1, 1) log: 1
2023/04/01 13:32:05 0 sendAppendEntries to 1 with pivot(1, 1) log: 1
2023/04/01 13:32:05 0 sendAppendEntries to 2 with pivot(1, 1) log: 1
2023/04/01 13:32:05 0 sendAppendEntries to 3 with pivot(1, 1) log: 1
2023/04/01 13:32:05 1 reply true and 3, update log length: 1
2023/04/01 13:32:05 2 reply true and 3, update log length: 1
2023/04/01 13:32:05 0 update 2 match 2 and next 3
2023/04/01 13:32:05 0 update 1 match 2 and next 3
2023/04/01 13:32:05 3 sendAppendEntries to 4 with pivot(51, 1) log: 0
2023/04/01 13:32:05 3 sendAppendEntries to 0 with pivot(1, 1) log: 50
2023/04/01 13:32:05 3 sendAppendEntries to 1 with pivot(1, 1) log: 50
2023/04/01 13:32:05 3 sendAppendEntries to 2 with pivot(1, 1) log: 50
2023/04/01 13:32:05 0 sendAppendEntries to 4 with pivot(1, 1) log: 1
2023/04/01 13:32:05 0 sendAppendEntries to 1 with pivot(2, 3) log: 0
2023/04/01 13:32:05 1 reply true and 3, update log length: 0
2023/04/01 13:32:05 0 sendAppendEntries to 2 with pivot(2, 3) log: 0
2023/04/01 13:32:05 2 reply true and 3, update log length: 0
2023/04/01 13:32:05 0 sendAppendEntries to 3 with pivot(1, 1) log: 1
2023/04/01 13:32:05 Start agreement: index 3
2023/04/01 13:32:05 3 sendAppendEntries to 4 with pivot(51, 1) log: 0
2023/04/01 13:32:05 3 sendAppendEntries to 2 with pivot(1, 1) log: 50
2023/04/01 13:32:05 3 sendAppendEntries to 0 with pivot(1, 1) log: 50
2023/04/01 13:32:05 3 sendAppendEntries to 1 with pivot(1, 1) log: 50
2023/04/01 13:32:05 4 start election(term: 2)
2023/04/01 13:32:05 4 send requestVote to 3
2023/04/01 13:32:05 4 send requestVote to 1
2023/04/01 13:32:05 4 send requestVote to 2
2023/04/01 13:32:05 4 send requestVote to 0
2023/04/01 13:32:05 0 sendAppendEntries to 4 with pivot(1, 1) log: 2
2023/04/01 13:32:05 0 sendAppendEntries to 2 with pivot(2, 3) log: 1
2023/04/01 13:32:05 2 reply true and 3, update log length: 1
2023/04/01 13:32:05 0 sendAppendEntries to 1 with pivot(2, 3) log: 1
2023/04/01 13:32:05 0 update 2 match 3 and next 4
2023/04/01 13:32:05 1 reply true and 3, update log length: 1
2023/04/01 13:32:05 0 update 1 match 3 and next 4
2023/04/01 13:32:05 0 sendAppendEntries to 3 with pivot(1, 1) log: 2
2023/04/01 13:32:05 3 sendAppendEntries to 4 with pivot(51, 1) log: 0
2023/04/01 13:32:05 3 sendAppendEntries to 0 with pivot(1, 1) log: 50
2023/04/01 13:32:05 3 sendAppendEntries to 1 with pivot(1, 1) log: 50
2023/04/01 13:32:05 3 sendAppendEntries to 2 with pivot(1, 1) log: 50
2023/04/01 13:32:05 0 sendAppendEntries to 4 with pivot(1, 1) log: 2
2023/04/01 13:32:05 0 sendAppendEntries to 1 with pivot(3, 3) log: 0
2023/04/01 13:32:05 0 sendAppendEntries to 2 with pivot(3, 3) log: 0
2023/04/01 13:32:05 2 reply true and 3, update log length: 0
2023/04/01 13:32:05 0 sendAppendEntries to 3 with pivot(1, 1) log: 2
2023/04/01 13:32:05 1 reply true and 3, update log length: 0
2023/04/01 13:32:05 Start agreement: index 4
...
Start agreement: index 51
2023/04/01 13:32:15 3 sendAppendEntries to 4 with pivot(51, 1) log: 0
2023/04/01 13:32:15 3 sendAppendEntries to 1 with pivot(1, 1) log: 50
2023/04/01 13:32:15 3 sendAppendEntries to 2 with pivot(1, 1) log: 50
2023/04/01 13:32:15 3 sendAppendEntries to 0 with pivot(1, 1) log: 50
2023/04/01 13:32:15 0 sendAppendEntries to 4 with pivot(1, 1) log: 50
2023/04/01 13:32:15 0 sendAppendEntries to 2 with pivot(50, 3) log: 1
2023/04/01 13:32:15 0 sendAppendEntries to 1 with pivot(50, 3) log: 1
2023/04/01 13:32:15 0 sendAppendEntries to 3 with pivot(1, 1) log: 50
2023/04/01 13:32:15 2 reply true and 3, update log length: 1
2023/04/01 13:32:15 1 reply true and 3, update log length: 1

```

It mainly because the test forces raft to complete the every agreement one by one, and my implementation only send appendEntries every 100ms. Here is the case that my raft have to wait 100ms to send next appendEntries, so the time for completing an agreement = 100ms + overhead.

Solution(I guess): trigger the appendEntries immediately after start an agreement, and set a larger timeout for periodically appendEntries.

However, in production, the timeout for regular appendEntries should be shorter than 100ms, and range from 0.5ms to 20ms depending on storage technology. Whether should raft trigger the appendEntries immediately is a trade-off here.

# Lab 2C

## Task:

**persist() and readPersist()**

This lab is easy and mainly counts on the correctness of implementation of 2A and 2B.

# Lab 2D

## Task:

Implement Snapshot() and the InstallSnapshot RPC

## Details:

**Implement Snapshot() first to pass the first test**

A good place to start is to assume the log starting at lastIncludedIndex. Initially set it to -1 and modify the code to pass 2B/2C tests. Then make Snapshot(index) discard the log before index, and set lastIncludedInde equal to index. But something need to be take care here is that the test call Snapshot(index) in:

```go
for m := range applyCh {
	...
	Snapshot(index)
}
```

Which means that we can not send thing to applyCh directly in Snapshot(index), or it will be blocked.

Furthermore, we must guarantee applying the snapshot before applying the command at (index + 1) to pass the test. You might observe that when calling Snapshot(index), the command at (index + 1) has been sent to applyCh before you can send snapshot (in my implememtation). 


## References:

1. Diego Ongaro and John Ousterhout (2014). "In Search of an Understandable Consensus Algorithm (Extended Version)." Technical Report No. 183, Stanford University. Available online: [https://raft.github.io/raft.pdf](https://raft.github.io/raft.pdf).
