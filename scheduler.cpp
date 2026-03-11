#include <iostream>
#include <fstream>
#include "json.hpp"
#include <vector>
#include <queue>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <chrono>

using namespace std;
using json = nlohmann::json;

struct Task {
    string id;
    string name;
    int priority;
    int duration;

    vector<string> depends_on;
    vector<string> children;

    int indegree = 0;
};

unordered_map<string, Task> tasks;

struct Compare {
    bool operator()(Task* a, Task* b) {
        return a->priority > b->priority;
    }
};

priority_queue<Task*, vector<Task*>, Compare> readyQueue;

mutex mtx;

void parseTasks(string filename)
{
    ifstream file(filename);
    json data;
    file >> data;

    for (auto &t : data["tasks"])
    {
        Task task;

        task.id = t["id"];
        task.name = t["name"];
        task.priority = t["priority"];
        task.duration = t["duration_ms"];

        for (auto &d : t["depends_on"])
            task.depends_on.push_back(d);

        tasks[task.id] = task;
    }
}

void buildGraph()
{
    for (auto &p : tasks)
    {
        string id = p.first;
        Task &task = p.second;

        for (auto &dep : task.depends_on)
        {
            tasks[dep].children.push_back(id);
            tasks[id].indegree++;
        }
    }
}

bool detectCycle()
{
    queue<string> q;
    unordered_map<string,int> indeg;

    for(auto &p : tasks)
    {
        indeg[p.first] = p.second.indegree;
    }

    for(auto &p : tasks)
    {
        if(indeg[p.first] == 0)
            q.push(p.first);
    }

    int count = 0;

    while(!q.empty())
    {
        string cur = q.front();
        q.pop();
        count++;

        for(auto &child : tasks[cur].children)
        {
            indeg[child]--;
            if(indeg[child] == 0)
                q.push(child);
        }
    }

    return count != tasks.size();
}

void worker(int id)
{
    while(true)
    {
        Task* task = nullptr;

        {
            lock_guard<mutex> lock(mtx);

            if(readyQueue.empty())
                return;

            task = readyQueue.top();
            readyQueue.pop();
        }

        cout<<"Worker "<<id<<" START "<<task->id<<endl;

        this_thread::sleep_for(
            chrono::milliseconds(task->duration)
        );

        cout<<"Worker "<<id<<" END "<<task->id<<endl;

        {
            lock_guard<mutex> lock(mtx);

            for(auto &child : task->children)
            {
                tasks[child].indegree--;

                if(tasks[child].indegree == 0)
                    readyQueue.push(&tasks[child]);
            }
        }
    }
}

int main()
{
    parseTasks("tasks.json");

    buildGraph();

    if(detectCycle())
    {
        cout<<"Cycle detected"<<endl;
        return 0;
    }

    for(auto &p : tasks)
    {
        if(p.second.indegree == 0)
            readyQueue.push(&p.second);
    }

    int workers = 4;

    vector<thread> pool;

    for(int i=0;i<workers;i++)
        pool.emplace_back(worker,i);

    for(auto &t : pool)
        t.join();
}