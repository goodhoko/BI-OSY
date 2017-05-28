#ifndef __PROGTEST__
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <cmath>
#include <cassert>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <deque>
#include <queue>
#include <stack>
#include <algorithm>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <array>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <mutex>
#include <memory>
#include <condition_variable>
#include <atomic>
using namespace std;

//=================================================================================================
class CLink
{
  public:
    //---------------------------------------------------------------------------------------------
                             CLink                         ( const string    & from,
                                                             const string    & to,
                                                             double            delay )
                             : m_From ( from ),
                               m_To ( to ),
                               m_Delay ( delay )
    {
    }
    //---------------------------------------------------------------------------------------------
    string                   m_From;
    string                   m_To;
    double                   m_Delay;
};
//=================================================================================================
class CCenter
{
  public:
    //---------------------------------------------------------------------------------------------
                             CCenter                       ( void )
    {
    }
    //---------------------------------------------------------------------------------------------
    virtual                  ~CCenter                      ( void )
    {
    }
    //---------------------------------------------------------------------------------------------
    void                     AddLink                       ( const CLink     & l )
    {
      m_Links . push_back ( l );
    }
    //---------------------------------------------------------------------------------------------
    friend ostream         & operator <<                   ( ostream         & os,
                                                             const CCenter   & x )
    {
      os << "Center: " << x . m_Center << ", Max delay: " << x . m_MaxDelay << '\n';
      for ( const auto & p : x . m_Delays )
        cout << '\t' << " -> " << p . first << ": " << p . second << '\n';
      return os;
    }
    //---------------------------------------------------------------------------------------------
    vector<CLink>            m_Links;
    string                   m_Center;
    double                   m_MaxDelay;
    map<string,double>       m_Delays;
};
typedef shared_ptr<CCenter> ACenter;
//=================================================================================================
class CRedundancy
{
  public:
    //---------------------------------------------------------------------------------------------
                             CRedundancy                   ( const string    & center )
                             : m_Center ( center )
    {
    }
    //---------------------------------------------------------------------------------------------
    virtual                  ~CRedundancy                  ( void )
    {
    }
    //---------------------------------------------------------------------------------------------
    void                     AddLink                       ( const CLink     & l )
    {
      m_Links . push_back ( l );
    }
    //---------------------------------------------------------------------------------------------
    friend ostream         & operator <<                   ( ostream         & os,
                                                             const CRedundancy & x )
    {
      for ( const auto & p : x . m_Redundancy )
        cout << x . m_Center << " - " << p . first << ": " << p . second << '\n';
      return os;
    }
    //---------------------------------------------------------------------------------------------
    
    vector<CLink>            m_Links;
    string                   m_Center;
    map<string,int>          m_Redundancy;
};
typedef shared_ptr<CRedundancy> ARedundancy;



//=================================================================================================
class CCustomer 
{
  public:
    //---------------------------------------------------------------------------------------------
    virtual                  ~CCustomer                    ( void )
    {
    }
    //---------------------------------------------------------------------------------------------
    virtual ACenter          GenCenter                     ( void ) = 0;
    //---------------------------------------------------------------------------------------------
    virtual ARedundancy      GenRedundancy                 ( void ) = 0;
    //---------------------------------------------------------------------------------------------
    virtual void             Solved                        ( ARedundancy       x ) = 0;
    //---------------------------------------------------------------------------------------------
    virtual void             Solved                        ( ACenter           x ) = 0;
    //---------------------------------------------------------------------------------------------
};
typedef shared_ptr<CCustomer> ACustomer;




//=================================================================================================
#endif /* __PROGTEST__ */


struct problem{
    ARedundancy redundancy;
    ACenter center;
    ACustomer customer;

    problem() : redundancy(NULL), center(NULL), customer(NULL) {}
};

class CSolver
{
  public:
                             CSolver                       ( void );
                             ~CSolver                      ( void );
    static void              Solve                         ( ACenter           x );
    static void              Solve                         ( ARedundancy       x );
    void                     Start                         ( int               thrCnt );
    void                     Stop                          ( void );
    void                     AddCustomer                   ( ACustomer         c );
  private:
    queue<problem>     		 problemQueue;
    vector<thread>			 consuments;
    mutex					 mtx;
    sem_t 		 		 	 full;
    sem_t 				 	 empty;
    atomic<int>				 producentCounter;
    condition_variable 		 cv;
    mutex 					 finishMtx;

    void 					 centerProducent			   (ACustomer customer); //true->center, false->redundancy
    void					 redundancyProducent		   (ACustomer customer); //true->center, false->redundancy
    void					 consumer					   ();
    static int 				 fordFulkerson				   (size_t s, size_t f, vector<vector<int>>& arr);
    static bool				 bfs						   (vector<vector<int>>& residual, size_t s, size_t t,vector<size_t>& parent);

};



CSolver::CSolver(){
	sem_init(&empty, 0, 0);
	sem_init(&full, 0, 10);
	producentCounter = 0;
}



CSolver::~CSolver(){
	sem_destroy(&empty);
	sem_destroy(&full);
}



void CSolver::Stop(){

	unique_lock<mutex> lock(finishMtx);
	while(producentCounter > 0){
		cv.wait(lock);
	}

	this->mtx.lock();
	this->problemQueue.push(problem());
	this->mtx.unlock();
	sem_post(&empty);

	for (vector<thread>::iterator i = this->consuments.begin(); i != this->consuments.end(); ++i){
		i->join();
	}
}



void CSolver::Start(int thrCnt){
	for (int i = 0; i < thrCnt; ++i){
		this->consuments.push_back(thread(&CSolver::consumer, this));
	}
}



void CSolver::consumer(){
	while(true){
		sem_wait(&empty);
		this->mtx.lock();
		problem problem = this->problemQueue.front();

		if(problem.center != NULL){
			this->problemQueue.pop();
			this->mtx.unlock();
			sem_post(&full);
			this->Solve(problem.center);
			problem.customer->Solved(problem.center);
			continue;
		}
		if(problem.redundancy != NULL){
			this->problemQueue.pop();
			this->mtx.unlock();
			sem_post(&full);
			this->Solve(problem.redundancy);
			problem.customer->Solved(problem.redundancy);
			continue;
		}
		cout << "Empty problem reached." << endl;
		this->mtx.unlock();
		sem_post(&empty);
		return;
	}
}



void CSolver::AddCustomer(ACustomer c){
	unique_lock<mutex> lock(finishMtx);
	thread t1(&CSolver::centerProducent, this, c);
	t1.detach();
	producentCounter++;
	thread t2(&CSolver::redundancyProducent, this, c);
	t2.detach();
	producentCounter++;
}



void CSolver::centerProducent(ACustomer customer){
	problem p;
	p.customer = customer;

	while(p.center = customer->GenCenter()){
		sem_wait(&full);
		this->mtx.lock();
			this->problemQueue.push(p);
		this->mtx.unlock();
		sem_post(&empty);
	}

	unique_lock<mutex> lock(finishMtx);
	producentCounter--;

	if(producentCounter == 0){
		lock.unlock();
		cv.notify_all();
	}
}



void CSolver::redundancyProducent(ACustomer customer){
	problem p;
	p.customer = customer;

	while(p.redundancy = customer->GenRedundancy()){
		sem_wait(&full);
		this->mtx.lock();
			this->problemQueue.push(p);
		this->mtx.unlock();
		sem_post(&empty);
	}

	unique_lock<mutex> lock(finishMtx);
	producentCounter--;

	if(producentCounter == 0){
		lock.unlock();
		cv.notify_all();
	}
}



void CSolver::Solve(ACenter x){
    map<string, size_t> str2idx;
    map<size_t, string> idx2str;
    vector<vector<double>> arr;
    size_t i, j, k, count = 0;
    //init maps for going from string to size_t and backwards
    for(auto &link : x->m_Links){
        if(!str2idx.count(link.m_From)){
            str2idx[link.m_From] = count;
            idx2str[count] = link.m_From;
            count++;
        }
        if(!str2idx.count(link.m_To)){
            str2idx[link.m_To] = count;
            idx2str[count] = link.m_To;
            count++;
        }
    }
    //initialize array for floyd-warshall, last column for holding max latency
    arr.resize(count);
    for(auto &vec : arr)
        vec.resize(count + 1);
    for(i = 0; i < count; i++){
        for(j = 0; j < count; j++){
            if(i != j && j != count)
                arr[i][j] = INFINITY;
        }
    }
    for(auto &link : x->m_Links){
        if(arr[str2idx[link.m_From]][str2idx[link.m_To]] > link.m_Delay){
            arr[str2idx[link.m_From]][str2idx[link.m_To]] = link.m_Delay;
            arr[str2idx[link.m_To]][str2idx[link.m_From]] = link.m_Delay;
        }
    }
    //floyd-warshall
    for (k = 0; k < count; k++){
        for (i = 0; i < count; i++){
            for (j = 0; j < count; j++){
                if (arr[i][k] + arr[k][j] < arr[i][j])
                    arr[i][j] = arr[i][k] + arr[k][j];
            }
        }
    }
    //finding center
    double min = INFINITY;
    size_t center;
    for (i = 0; i < count; i++){
        for (j = 0; j < count; j++){
            if(arr[i][j] > arr[i][count])
                arr[i][count] = arr[i][j];
        }
        if(arr[i][count] < min){
            min = arr[i][count];
            center = i;
        }
    }
    //finish
    x->m_Center = idx2str[center];
    x->m_MaxDelay = arr[center][count];
    for (i = 0; i < count; i++){
        if(i != center)
            x->m_Delays[idx2str[i]] = arr[center][i];
    }
}



void CSolver::Solve ( ARedundancy x ){
    map<string, size_t> str2idx;
    map<size_t, string> idx2str;
    vector<vector<int>> arr;
    size_t i, count = 0;

    for(auto &link : x->m_Links){
        if(!str2idx.count(link.m_From)){
            str2idx[link.m_From] = count;
            idx2str[count] = link.m_From;
            count++;
        }
        if(!str2idx.count(link.m_To)){
            str2idx[link.m_To] = count;
            idx2str[count] = link.m_To;
            count++;
        }
    }

    arr.resize(count);
    for(auto &vec : arr)
        vec.resize(count + 1);

    for(auto &link : x->m_Links){
            arr[str2idx[link.m_From]][str2idx[link.m_To]] ++;
            arr[str2idx[link.m_To]][str2idx[link.m_From]] ++;
    }

    for (i = 0; i < count; ++i){
        if(i != str2idx[x->m_Center])
            x->m_Redundancy[idx2str[i]] = fordFulkerson(str2idx[x->m_Center], i, arr);
    }
}



int CSolver::fordFulkerson(size_t s, size_t f, vector<vector<int>>& arr){
    size_t u, v;
    int redundancy = 0;
    vector<vector<int>> residual = arr;
    vector<size_t> parent;
    parent.resize(residual.size());
 
    while (bfs(residual, s, f, parent)){
        int path_flow = INT_MAX;
        for (v=f; v!= s; v=parent[v]){
            u = parent[v];
            path_flow = min(path_flow, residual[u][v]);
        }
        for (v=f; v != s; v=parent[v]){
            u = parent[v];
            residual[u][v] -= path_flow;
            residual[v][u] += path_flow;
        }
        redundancy += path_flow;
    }
 
    return redundancy;
}



bool CSolver::bfs(vector<vector<int>>& residual, size_t s, size_t f, vector<size_t>& parent){
    vector<bool> visited;
    visited.resize(residual.size());
    queue <size_t> q;
    q.push(s);
    visited[s] = true;
    parent[s] = -1;
 
    while (!q.empty()){
        int u = q.front();
        q.pop();
        for (size_t v=0; v<residual.size(); v++){
            if (visited[v]==false && residual[u][v] > 0){
                q.push(v);
                parent[v] = u;
                visited[v] = true;
            }
        }
    }

    return visited[f];
}


//=================================================================================================
#ifndef __PROGTEST__
#include "data.inc"
#include "test.inc"
#endif /* __PROGTEST__ */