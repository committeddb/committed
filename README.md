# Committed

![Build Status](https://app.codeship.com/projects/28313820-8549-0137-dc4f-2289976f1a49/status?branch=master)

Committed is a distributed commit log designed to store data long term in a log structure. Instead of the typical implementation where you are given simple read and write primitives and have to build, use adddons, or 3rd party software to aid in your read activites, Committed's two primitives are write and sync. The sync primitive is designed to move your data somewhere else. The purpose of this is to make it easy to transform or multiplex streams of data in a value added manner, or to move data into a system that has efficient querying (like a traditional SQL or NoSQL database). Committed even works with ephemeral data storage because it provides an efficient way to recreate the ephemeral storage if it fails (think Redis).

Another way to think of Committed is as a serious system for creating distributed Command Query Responsibility Segregation (CQRS) systems. In this case Committed would work as the Command database and provide powerful semantics for replicating transformed data into Query systems.

A final way to think of Committed is as a functional database with powerful data stream transformation capabilities.

Committed is specifically NOT a databse designed for querying.

## Version 0.1
This is the MVP of Committed. You can spin up a cluster. See the included Procfile for an example of how to run the server or just run the Procfile directly using goreman or something similar that can execute Procfiles.

Once the server is running you should see 3 leader elections (if you change the number of nodes you should see every node come to consensus). When the server is working your terminal should look something like this:

```
08:13:19 committed1 | raft2018/04/30 08:13:19 INFO: raft.node: 1 elected leader 1 at term 2  
08:13:19 committed2 | raft2018/04/30 08:13:19 INFO: raft.node: 2 elected leader 1 at term 2  
08:13:19 committed3 | raft2018/04/30 08:13:19 INFO: raft.node: 3 elected leader 1 at term 2  
```

The term number doesn't matter and who got elected leader doesn't matter either.

Once started committed will create two folders per node. The first node will be called raft-node# and raft-node#-snap. These folders contain all of your data. To clear the data in the server you can simply delete the folders.

The MVP has three things you can do:

* Add a topic
* Append to a topic
* Add a SQL syncable

To Add a topic POST to http://server:port/cluster/topics with an HTTP body that looks like:  
{  
	"Name" : "test",  
	"NodeCount" : 1  
}  
Node count does not do anything in the MVP so the value doesn't matter

To Append to a topic POST to http://server:port/cluster/posts with an HTTP body that looks like:  
{  
	"Topic" : "test",  
	"Proposal" : "My Data"  
}  

To Add a syncable POST to http://server:port/cluster/syncables with an HTTP body that looks like:  
{  
	"Style": "toml",  
	"Syncable": "IyBEZXRlcm1pbmVzIHdoYXQgdGhlIHJlc3Qgb2YgdGhlIGNvbmZpZyB3aWxsIGxvb2sgbGlrZQ0KW2RiXQ0KdHlwZSA9ICJzcWwiDQoNCltzcWxdDQpkcml2ZXIgPSAicWwiDQpjb25uZWN0aW9uU3RyaW5nID0gIm1lbW9yeTovL2ZvbyINCg0KW3NxbC50b3BpY10NCm5hbWUgPSAidGVzdDEiDQoNCltbc3FsLnRvcGljLm1hcHBpbmddXQ0KanNvblBhdGggPSAiJC5LZXkiDQp0YWJsZSA9ICJmb28iDQpjb2x1bW4gPSAia2V5Ig0KDQpbW3NxbC50b3BpYy5tYXBwaW5nXV0NCmpzb25QYXRoID0gIiQuT25lIg0KdGFibGUgPSAiZm9vIg0KY29sdW1uID0gInR3byI="  
}  
The Syncable is a Base64 encode of a file. The file can be any type that Viper supports which at this time is JSON, TOML, YAML, HCL, and Java properties config files. In the Style field put what file type you have encoded. For an example of what a syncable looks like look at the simple.toml file in the syncable folder of this project.

The workflow would look like: Add a topic, Append some proposals, Add a syncable, look at your SQL database and see the proposals get applied to the DB, Add some more proposals and then read the database and do something useful.

Lastly there is a web UI that can be accessed by hitting the root of the server. For instance if you are running on localhost port 12380 you can point a web browser at http://localhost:12380/ to see the UI. The UI purposefully is read only. To interact with the server please use the web API described above.

## Road Map

Version 0.2 will be delivered by May 31st.

It will include (Strikethrough items are alread completed):

Backend
* A topic syncable that allows you to transform data on one topic and write the transformed data to a new topic

Also before May 31st I will setup an online sandbox that will allow users to play with a shared server without having to download the database by themselves.
