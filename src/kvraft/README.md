# Lab 3A

## Block point:

1. If you plan to transfer the result by channel, you need to make sure that the channel exists during the transfer and close the channel after the RPC.

# Lab 3B

## Block point:

1. if you try to just compare the maxraftstate with persister.RaftStateSize(), and trigger the snapshot, it may cause a problem. Image when server is accepting commands from applyCh, Raft is also starting new agreements at the same time. Assuming index 20 at Raft'log is a threshold to trigger the snapshot, server 1 and 2 may choose to snapshot at index 19 or 20, because of the delays of accepting commands from applyCh. Thus, setting a checkpoint(how many logs can we trigger a snapshot) and calculating the ratio of maxraftstate and persister.RaftStateSize() should be a better option.
2. What should be persisted?

## Later work:

1. In my implementation, the leader sends snapshot to the follower when follower's log is just one index behind and the follower is going to take a snapshot. So it is better to allow the follower's log lag a few logs behind the leader.
2. The number of RPC need to be optimized.
