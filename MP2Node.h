/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

#define ABORT
class Transaction {
public:
	int transID_;
	int count_;
	MessageType type_;
	string key_;
	string value_;
	Transaction(int transID, MessageType type) : transID_(transID), count_(-1), type_(type) {
	}
	Transaction(int transID, MessageType type, string key, string value = "") : transID_(transID), count_(-1), type_(type), key_(key), value_(value) {
	}
	void increCount() {
		count_ += 1;
	}
	void startCount() {
		count_ = 0;
	}
	bool isStart() {
		return -1 == count_;
	}
};

class ReadTransaction {
public:
	int transID_;
	int count_;
	MessageType type_;//TODO: is it necessaray?
	string key_;
	vector< pair<string, int> > values_;
	ReadTransaction(int transID, string key) : transID_(transID), count_(-1), key_(key) {
	}
	void increCount() {
		count_ += 1;
	}
	void startCount() {
		count_ = 0;
	}
	bool isStart() {
		return -1 == count_;
	}
	void pushValue(string value) {
		for (auto it = values_.begin(); it != values_.end(); ++it) {
			if (it->first == value)
			{
				it->second += 1;
				return;
			}
		}
		values_.push_back(make_pair(value, 1));
		return;
	}
};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	// buff for
	list<Transaction> buff;
	// read buff
	list<ReadTransaction> buffRead;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	void printAddress(Address *addr);

	~MP2Node();
};

#endif /* MP2NODE_H_ */
