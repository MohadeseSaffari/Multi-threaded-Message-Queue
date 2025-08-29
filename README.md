Multi-threaded Message Queue
Introduction and Project Goal

The goal of this project is to design and implement a simple Message Queue system.
Such a system resembles a Message Broker (like Kafka or RabbitMQ) that works in the real world and enables communication between multiple processes or threads.

In this project, threads can communicate with each other concurrently.

This project helps you understand and implement the following core concepts in practice:

Thread Management – Creating and managing multiple threads simultaneously.

Producer–Consumer Problem – Implementing the classic producer–consumer problem, where multiple producers generate messages and multiple consumers consume them.

Synchronization – Using tools such as Mutex and Condition Variables to ensure safe access to shared resources and prevent race conditions.

Thread-safe Data Structures – Designing data structures (like a queue) that can safely be accessed by multiple threads.

Overall System Architecture

The goal is to develop a task distribution engine that receives tasks and distributes them to workers.

Tasks belong to topics, each with a name, priority, and estimated execution time.

Each worker subscribes to multiple topics and gets assigned tasks.

The distributor (message broker) monitors task queues, receives completed task status from workers, and assigns new tasks based on:

Execution time

Priority

Current load of each worker

Priority assumption:

High-priority tasks must be executed earlier,

But low-priority tasks should still be load-balanced fairly among workers.

Deadlines:

Sometimes a task may have a deadline.

If the task cannot be completed within the deadline, the distributor should reject it.

The system is composed of the following main parts:

Message Broker – The main program that manages all threads and topics.

Topic – A communication channel (message queue) dedicated to a specific subject. Each topic stores related messages.

Producer – A thread responsible for generating messages and adding them to a topic queue. There can be multiple producers working simultaneously.

Consumer – A thread responsible for retrieving messages from one or more topic queues and processing them. Multiple consumers can exist and run concurrently.

Implementation Steps
Step 1 – Define Structures

First, define the required data structures:

Message – A simple structure containing an ID and content (e.g., a string).

Queue – Implemented using a linked list for the topic’s message queue.

Topic – The core structure of the system, containing:

A message queue

A Mutex lock for enqueue/dequeue operations

A Condition Variable (not_empty) for consumers to wait when the queue is empty

Step 2 – Implement Producer Logic

Write a function for producer threads that runs in a loop:

Create a new message.

Add the message to the topic queue:

Lock the queue mutex

Append the message to the queue

Signal the not_empty condition variable (wakes up any waiting consumers)

Unlock the mutex

Sleep briefly, then repeat.

Step 3 – Implement Consumer Logic

Write a function for consumer threads that runs in a loop:

Remove a message from the topic queue:

Lock the queue mutex

Wait on the not_empty condition variable while the queue is empty (this automatically releases the mutex and reacquires it after wake-up)

Take the first message from the queue

Unlock the mutex

Process the message (e.g., print it).

Step 4 – Main Program

In the main function:

Create and initialize multiple topics (e.g., 2).

Create multiple producer and consumer threads working on different topics.

Run all threads.

Wait for all threads to finish using pthread_join.

Design a simple dashboard to:

Display the status of queues and workers

Show the average execution time of tasks by priority

Step 5 – Extensions (Optional Features)

You may add extra capabilities as system extensions. Each feature should be configurable and disabled if its control flag is false. Examples:

Bounded Buffer (Limited Queue Capacity):

Each topic has a maximum size.

If the queue is full, producers must wait until consumers free up space.

Requires an additional condition variable (not_full).

Retention Policy (Message Expiration):

Messages have a Time-To-Live (TTL).

A separate thread (Garbage Collector) periodically checks queues and removes expired messages.

Fan-out (Multiple Consumers per Topic):

Support multiple consumers subscribing to the same topic.

Messages can be delivered to multiple consumers (broadcast).
