Distributed Key value store (WIP)

Features
- Can spawn multiple nodes
- Coordinator will hash key to node (Consistent hashing)
- Hashtable with RB trees
- Data persistance and recovery (WAL and checkpoints)
- Config driven
- Coordinator CLI

Build
- Proto bindings: `make proto`
- Node and Coordinaor: `make`

Example
```
(shell) ./build/node -config=./config/node-config-2.yaml &
[1] 88831
(shell) ./build/node -config=./config/node-config-1.yaml &
(shell) ./build/coordinator -config=./config/coordinator-config.yaml
>> PUT SampleKey1 SampleValue1
Node[StorageNode2] Is Updated : true
>> PUT SampleKey2 SampleValue2
Node[StorageNode1] Is Updated : true
>> PUT SampleKey3 SampleValue3
Node[StorageNode1] Is Updated : true
>> GET SampleKey1
Node[StorageNode2] Value : SampleValue1
>> GET SampleKey2
Node[StorageNode1] Value : SampleValue2
>> UPDATE SampleKey2 SampleValue2_
Node[StorageNode1] Update Status : true
>> GET SampleValue2
Node[StorageNode2] Value :
>> GET SampleKey2
Node[StorageNode1] Value : SampleValue2_
>> DELETE SampleKey2
Node[StorageNode1] Delete Status : true
>> GET SampleKey2
Node[StorageNode1] Value :
>>

```

Direcotory structure
```
├── Makefile
├── ReadMe.md
├── build // Binaries
├── cmd
│   ├── coordinator
│   └── node
├── config // Sample configuration files
├── gen // Autogenerated proto files
├── go.mod
├── go.sum
├── internal
│   ├── coordinator
│   ├── node
│   └── utils // Utilities
├── proto // Proto files
└── test // basic unit tests (Not exhaustive)

```