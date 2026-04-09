# Publisher-Subscriber-CN_Project


OUTPUT SUBSCRIBER SIDE

1. subscribing 

anchitachhibba@Anchitas-MacBook-Air Publisher-Subscriber-CN_Project % python3 subscriber.py 
[CONNECTED] Broker at localhost:9010
[SSL] Protocol: TLSv1.3  |  Cipher: TLS_AES_256_GCM_SHA384

[LISTENING] Waiting for messages...


[TOPICS] Available: ['sports', 'tech', 'finance', 'weather']
Choice: 
==== Subscriber Menu (SSL secured) ====
1. Subscribe to topic
2. Unsubscribe from topic
3. Show / Refresh available topics
4. View broker stats
5. Exit
Choice: 1

[TOPICS] Available: ['sports', 'tech', 'finance', 'weather']
Choice: Available topics : ['sports', 'tech', 'finance', 'weather']
Your subscriptions: []
Enter topic to subscribe : sports
[SUBSCRIBED] → 'sports'

2. unsubscribing

==== Subscriber Menu (SSL secured) ====
1. Subscribe to topic
2. Unsubscribe from topic
3. Show / Refresh available topics
4. View broker stats
5. Exit
Choice: 2
Your subscriptions: ['sports', 'tech']
Enter topic to unsubscribe: tech 
[UNSUBSCRIBED] ✗ 'tech'

3. Viewing subscribed topics

==== Subscriber Menu (SSL secured) ====
1. Subscribe to topic
2. Unsubscribe from topic
3. Show / Refresh available topics
4. View broker stats
5. Exit
Choice: 3

[TOPICS] Available: ['sports', 'tech', 'finance', 'weather']
Choice: [INFO] Available topics    : ['sports', 'tech', 'finance', 'weather']
[INFO] Your subscriptions  : ['sports', 'finance']

4. Viewing broker stats
==== Subscriber Menu (SSL secured) ====
1. Subscribe to topic
2. Unsubscribe from topic
3. Show / Refresh available topics
4. View broker stats
5. Exit
Choice: 4

[BROKER STATS]
  uptime               352.0s
  published            0
  delivered            0
  clients              2
  throughput           0.00msg/s
  [local] received     0
  [local] rate         0.00 msg/s

OUTPUT PUBLISHER SIDE

1. Manual Publish
==== Publisher Menu (SSL secured) ====
Active topics : ['sports', 'tech', 'finance', 'weather']
1. Manual Publish
2. Create New Topic
3. Auto News Stream
4. Stress Test (performance benchmark)
5. Exit
Choice: 1
Enter topic name (existing or new): sports
Message: RCB won by 3 wickets against RR
[SENT] 'sports' → RCB won by 3 wickets against RR  (send time: 1.11 ms)

════════════════════════════════════════════
║ [SPORTS] RCB won by 3 wickets against RR ║
║  14:42:40  |  msg #1                     ║
════════════════════════════════════════════

2. creating a new topic 
==== Publisher Menu (SSL secured) ====
Active topics : ['sports', 'tech', 'finance', 'weather']
1. Manual Publish
2. Create New Topic
3. Auto News Stream
4. Stress Test (performance benchmark)
5. Exit
Choice: 2
New topic name: India news
[CREATED] Topic 'India news'
[CREATED] Topic 'India news' is now active.

3. Auto news 
==== Publisher Menu (SSL secured) ====
Active topics : ['sports', 'tech', 'finance', 'weather', 'India news']
1. Manual Publish
2. Create New Topic
3. Auto News Stream
4. Stress Test (performance benchmark)
5. Exit
Choice: 3
Delay between messages (seconds, default 2): 
[AUTO MODE] Streaming every 2.0s — Ctrl+C to stop

[AUTO] India news → Update on India news  |  total: 1  rate: 1608.2 msg/s
[AUTO] weather → Cyclone approaching coast  |  total: 2  rate: 1.0 msg/s
[AUTO] weather → Cyclone approaching coast  |  total: 3  rate: 0.7 msg/s
[AUTO] India news → Update on India news  |  total: 4  rate: 0.7 msg/s
^C
[AUTO STOPPED] Sent 4 messages in 7.0s (0.57 msg/s)

4. Stress test
==== Publisher Menu (SSL secured) ====
Active topics : ['sports', 'tech', 'finance', 'weather', 'India news']
1. Manual Publish
2. Create New Topic
3. Auto News Stream
4. Stress Test (performance benchmark)
5. Exit
Choice: 4
Number of messages to send: 20

[STRESS TEST] Launching 20 concurrent publish threads...

[STRESS TEST RESULTS]
  Messages sent : 20
  Total time    : 0.0047s
  Throughput    : 4219.20 msg/s
  Avg latency   : 0.2370 ms/msg

 