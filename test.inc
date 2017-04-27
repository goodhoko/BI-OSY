//=================================================================================================
class CCustomerReal : public CCustomer
{
  public:
    //---------------------------------------------------------------------------------------------
                             CCustomerReal                 ( void )
                             : m_IdxCenter ( 0 ),
                               m_IdxRedundancy ( 0 )
    {
    }
    //---------------------------------------------------------------------------------------------
    virtual ACenter          GenCenter                     ( void ) override
    {
      if ( m_IdxCenter < g_TestsCenter . size () )
        return g_TestsCenter[m_IdxCenter++] . first ();
      else
        return ACenter ();
    }
    //---------------------------------------------------------------------------------------------
    virtual ARedundancy      GenRedundancy                 ( void ) override
    {
      if ( m_IdxRedundancy < g_TestsRedundancy . size () )
        return g_TestsRedundancy[m_IdxRedundancy++] . first ();
      else
        return ARedundancy ();
    }
    //---------------------------------------------------------------------------------------------
    virtual void             Solved                        ( ARedundancy       x ) override
    {
      // todo: validate results
    }
    //---------------------------------------------------------------------------------------------
    virtual void             Solved                        ( ACenter           x ) override
    {
      // todo: validate results
    }
    //---------------------------------------------------------------------------------------------
  private:
    size_t                   m_IdxCenter;
    size_t                   m_IdxRedundancy;
};
//=================================================================================================
void               TestParallel                            ( void )
{
  CSolver   s;
  
  s . AddCustomer ( make_shared<CCustomerReal> () );
  s . Start ( 2 );
  s . AddCustomer ( make_shared<CCustomerReal> () );
  s . Stop ();
}
//=================================================================================================
void               TestSequential                          ( void )
{
  for ( auto & t : g_TestsCenter )
  {
    ACenter x = t . first ();
    CSolver::Solve ( x );
    t . second ( x );
  }

  for ( auto & t : g_TestsRedundancy )
  {
    ARedundancy x = t . first ();
    CSolver::Solve ( x );
    t . second ( x );
  }
}
//=================================================================================================
int                main                                    ( void )
{
  TestSequential ();
  TestParallel ();
  return 0;
}
