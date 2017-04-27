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

class CLink
{
  public:
    string                   m_From;
    string                   m_To;
    double                   m_Delay;

    CLink (const string& from, const string& to, double delay) : m_From( from ), m_To ( to ), m_Delay ( delay ) {}
};




class CCenter
{
  public:
    vector<CLink>            m_Links;
    string                   m_Center;
    double                   m_MaxDelay;
    map<string,double>       m_Delays;

    CCenter ( void ){}
    virtual ~CCenter ( void ){}
    void AddLink( const CLink     & l ) {
      	m_Links . push_back ( l );
    }
    friend ostream& operator << ( ostream& os, const CCenter   & x ){
      	os << "Center: " << x . m_Center << ", Max delay: " << x . m_MaxDelay << '\n';
      	for ( const auto & p : x . m_Delays )
      	  	cout << '\t' << " -> " << p . first << ": " << p . second << '\n';
      	return os;
    }
};
typedef shared_ptr<CCenter> ACenter;




class CRedundancy
{
  public:
    vector<CLink>            m_Links;
    string                   m_Center;
    map<string,int>          m_Redundancy;

	CRedundancy ( const string    & center ) : m_Center ( center ) {}
	virtual CRedundancy ( void ) {}
	void AddLink ( const CLink& l ) {
      	m_Links . push_back ( l );
    }
	friend ostream& operator << ( ostream& os, const CRedundancy & x ) {
      	for ( const auto & p : x . m_Redundancy )
        	cout << x . m_Center << " - " << p . first << ": " << p . second << '\n';
      	return os;
    }
	     
};
typedef shared_ptr<CRedundancy> ARedundancy;




class CCustomer 
{
  public:
    virtual ~CCustomer ( void ) {}
    virtual ACenter GenCenter ( void ) = 0;
    virtual ARedundancy GenRedundancy ( void ) = 0;
    virtual void Solved ( ARedundancy       x ) = 0;
    virtual void Solved ( ACenter           x ) = 0;
};
typedef shared_ptr<CCustomer> ACustomer;

#endif /* __PROGTEST__ */


//=============================================================================================


class CSolver
{
public:
    CSolver ( void );
    ~CSolver ( void );
    static void Solve ( ACenter x );
    static void Solve ( ARedundancy x );
    void Start ( int thrCnt );
    void Stop ( void );
    void AddCustomer ( ACustomer c );
  private:
    
};



static void CSolver::Solve(ACenter x){
	
}




#ifndef __PROGTEST__
#include "data.inc"
#include "test.inc"
#endif /* __PROGTEST__ */
