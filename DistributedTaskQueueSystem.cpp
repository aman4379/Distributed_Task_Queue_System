#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketStream.h>
#include <Poco/Util/Application.h>
#include <Poco/Thread.h>
#include <Poco/ThreadPool.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketAcceptor.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/SocketReactor.h>
#include <iostream>
#include <string>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <bsoncxx/json.hpp>
#include <vector>
#include <atomic>

using namespace Poco;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_document;
using bsoncxx::builder::stream::close_document;

struct Task {
    std::string id;
    std::string data;
};

mongocxx::instance inst{};

mongocxx::client client{mongocxx::uri{"mongodb://localhost:27017"}};
mongocxx::database db = client["task_queue"];
mongocxx::collection tasks = db["tasks"];

class WorkerNode : public Runnable {
public:
    WorkerNode(const std::string& name)
        : name_(name) {}

    void run() {
        while (true) {
            Task task = taskQueue_.getTask();
            std::cout << "Worker " << name_ << " executing task: " << task.id << std::endl;
            Thread::sleep(1000);
            std::cout << "Worker " << name_ << " completed task: " << task.id << std::endl;
        }
    }

private:
    std::string name_;
};

class TaskQueue {
public:
    TaskQueue() : nextWorker_(0) {}

    void addTask(const Task& task) {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_.push(task);
        condition_.notify_one();
    }

    Task getTask() {
        int workerIndex = nextWorker_.fetch_add(1) % workers_.size();
        Task task = tasks_.front();
        tasks_.pop();

        workers_[workerIndex]->assignTask(task);

        return task;
    }

    void addWorker(WorkerNode* worker) {
        workers_.push_back(worker);
    }

private:
    std::vector<WorkerNode*> workers_;
    std::queue<Task> tasks_;
    std::mutex mutex_;
    std::condition_variable condition_;
    std::atomic<int> nextWorker_;
};

class TaskServer {
public:
    TaskServer(TaskQueue& taskQueue, int port)
        : taskQueue_(taskQueue), port_(port) {}

    void start() {
        Net::ServerSocket serverSocket(port_);
        Net::SocketAcceptor<TaskServer> acceptor(serverSocket, *this);

        std::cout << "Task server started on port " << port_ << std::endl;

        Thread thread;
        thread.start(acceptor);
        thread.join();
    }

    void handleConnection(Net::StreamSocket& socket) {
        Net::SocketStream stream(socket);
        Task task;
        stream >> task.id >> task.data;

        bsoncxx::document::value doc = document{} <<
            "id" << task.id <<
            "data" << task.data << finalize;
        tasks.insert_one(doc.view());

        taskQueue_.addTask(task);
    }

private:
    TaskQueue& taskQueue_;
    int port_;
};

int main() {
    TaskQueue taskQueue;

    std::vector<WorkerNode> workers;
    for (int i = 1; i <= 3; ++i) {
        std::string workerName = "Worker-" + std::to_string(i);
        WorkerNode worker(workerName);
        workers.push_back(worker);
        Thread thread;
        thread.start(worker);
        taskQueue.addWorker(&worker);
    }

    TaskServer taskServer(taskQueue, 8080);
    taskServer.start();

    return Application::EXIT_OK;
}
