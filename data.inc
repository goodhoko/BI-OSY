//-------------------------------------------------------------------------------------------------
ACenter            GenCenter0                              ( void )
{
  ACenter x = make_shared<CCenter> ();
  x -> AddLink ( CLink ( "node_0", "node_1", 10 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 19.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 20 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 22.75 ) );
  x -> AddLink ( CLink ( "node_0", "node_1", 19.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 12 ) );
  x -> AddLink ( CLink ( "node_3", "node_1", 13 ) );
  x -> AddLink ( CLink ( "node_0", "node_3", 0.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_0", 20 ) );
  x -> AddLink ( CLink ( "node_0", "node_2", 3.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_4", 5.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_4", 15.5 ) );
  return x;
}
//-------------------------------------------------------------------------------------------------
void               ValidateCenter0                         ( ACenter           x )
{
  assert ( x -> m_Center == "node_0" );
  assert ( x -> m_MaxDelay == 10 );
  assert ( x -> m_Delays["node_1"] == 10 );
  assert ( x -> m_Delays["node_2"] == 3.5 );
  assert ( x -> m_Delays["node_3"] == 0.75 );
  assert ( x -> m_Delays["node_4"] == 5.75 );
}
//-------------------------------------------------------------------------------------------------
ACenter            GenCenter1                              ( void )
{
  ACenter x = make_shared<CCenter> ();
  x -> AddLink ( CLink ( "node_0", "node_1", 16 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 13.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 12.5 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 24.25 ) );
  x -> AddLink ( CLink ( "node_4", "node_5", 7.5 ) );
  x -> AddLink ( CLink ( "node_5", "node_6", 19.25 ) );
  x -> AddLink ( CLink ( "node_3", "node_5", 10.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_1", 9 ) );
  x -> AddLink ( CLink ( "node_5", "node_6", 2 ) );
  x -> AddLink ( CLink ( "node_6", "node_3", 2.5 ) );
  x -> AddLink ( CLink ( "node_1", "node_4", 22.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_0", 0.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_0", 6.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_1", 13.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_5", 13 ) );
  x -> AddLink ( CLink ( "node_4", "node_3", 1.25 ) );
  x -> AddLink ( CLink ( "node_3", "node_6", 23.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_1", 18.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_2", 17.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_3", 22 ) );
  x -> AddLink ( CLink ( "node_5", "node_2", 6 ) );
  x -> AddLink ( CLink ( "node_6", "node_2", 17.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_4", 16.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_3", 23 ) );
  return x;
}
//-------------------------------------------------------------------------------------------------
void               ValidateCenter1                         ( ACenter           x )
{
  assert ( x -> m_Center == "node_6" );
  assert ( x -> m_MaxDelay == 9 );
  assert ( x -> m_Delays["node_0"] == 8.75 );
  assert ( x -> m_Delays["node_1"] == 9 );
  assert ( x -> m_Delays["node_2"] == 8 );
  assert ( x -> m_Delays["node_3"] == 2.5 );
  assert ( x -> m_Delays["node_4"] == 3.75 );
  assert ( x -> m_Delays["node_5"] == 2 );
}
//-------------------------------------------------------------------------------------------------
ACenter            GenCenter2                              ( void )
{
  ACenter x = make_shared<CCenter> ();
  x -> AddLink ( CLink ( "node_0", "node_1", 20.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 17.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 22.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 12.25 ) );
  x -> AddLink ( CLink ( "node_4", "node_5", 5.75 ) );
  x -> AddLink ( CLink ( "node_5", "node_6", 23.75 ) );
  x -> AddLink ( CLink ( "node_6", "node_7", 23 ) );
  x -> AddLink ( CLink ( "node_7", "node_8", 4 ) );
  x -> AddLink ( CLink ( "node_8", "node_9", 22 ) );
  x -> AddLink ( CLink ( "node_9", "node_10", 16 ) );
  x -> AddLink ( CLink ( "node_4", "node_6", 7.25 ) );
  x -> AddLink ( CLink ( "node_8", "node_3", 11.25 ) );
  x -> AddLink ( CLink ( "node_3", "node_6", 10.5 ) );
  x -> AddLink ( CLink ( "node_1", "node_9", 3 ) );
  x -> AddLink ( CLink ( "node_1", "node_5", 19 ) );
  x -> AddLink ( CLink ( "node_7", "node_4", 18.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_3", 6 ) );
  x -> AddLink ( CLink ( "node_6", "node_2", 4 ) );
  x -> AddLink ( CLink ( "node_8", "node_1", 19.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_8", 2.25 ) );
  x -> AddLink ( CLink ( "node_10", "node_0", 13.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 20 ) );
  x -> AddLink ( CLink ( "node_8", "node_7", 24 ) );
  x -> AddLink ( CLink ( "node_7", "node_8", 2.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_5", 2.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_2", 11.75 ) );
  x -> AddLink ( CLink ( "node_9", "node_6", 19 ) );
  x -> AddLink ( CLink ( "node_0", "node_1", 24.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_9", 3.5 ) );
  x -> AddLink ( CLink ( "node_10", "node_0", 21.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_6", 4.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_4", 22.75 ) );
  x -> AddLink ( CLink ( "node_9", "node_3", 14 ) );
  x -> AddLink ( CLink ( "node_6", "node_4", 17.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_5", 19 ) );
  x -> AddLink ( CLink ( "node_3", "node_10", 14.5 ) );
  x -> AddLink ( CLink ( "node_9", "node_8", 15.75 ) );
  x -> AddLink ( CLink ( "node_0", "node_8", 20.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_9", 20.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_8", 22.5 ) );
  x -> AddLink ( CLink ( "node_10", "node_7", 12.5 ) );
  x -> AddLink ( CLink ( "node_1", "node_9", 22.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_7", 6.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_2", 17.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_0", 16 ) );
  x -> AddLink ( CLink ( "node_2", "node_7", 13 ) );
  x -> AddLink ( CLink ( "node_10", "node_3", 13.75 ) );
  x -> AddLink ( CLink ( "node_7", "node_1", 12 ) );
  x -> AddLink ( CLink ( "node_6", "node_10", 11.5 ) );
  x -> AddLink ( CLink ( "node_3", "node_9", 11 ) );
  x -> AddLink ( CLink ( "node_0", "node_3", 15 ) );
  x -> AddLink ( CLink ( "node_9", "node_2", 17 ) );
  return x;
}
//-------------------------------------------------------------------------------------------------
void               ValidateCenter2                         ( ACenter           x )
{
  assert ( x -> m_Center == "node_2" );
  assert ( x -> m_MaxDelay == 14.5 );
  assert ( x -> m_Delays["node_0"] == 11.75 );
  assert ( x -> m_Delays["node_1"] == 6.5 );
  assert ( x -> m_Delays["node_10"] == 8.75 );
  assert ( x -> m_Delays["node_3"] == 14.5 );
  assert ( x -> m_Delays["node_4"] == 11.25 );
  assert ( x -> m_Delays["node_5"] == 8.75 );
  assert ( x -> m_Delays["node_6"] == 4 );
  assert ( x -> m_Delays["node_7"] == 11.5 );
  assert ( x -> m_Delays["node_8"] == 8.75 );
  assert ( x -> m_Delays["node_9"] == 3.5 );
}
//-------------------------------------------------------------------------------------------------
ACenter            GenCenter3                              ( void )
{
  ACenter x = make_shared<CCenter> ();
  x -> AddLink ( CLink ( "node_0", "node_1", 12.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 7.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 17.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 4.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_5", 15.5 ) );
  x -> AddLink ( CLink ( "node_5", "node_6", 1.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_7", 10.5 ) );
  x -> AddLink ( CLink ( "node_7", "node_8", 17.5 ) );
  x -> AddLink ( CLink ( "node_8", "node_9", 17 ) );
  x -> AddLink ( CLink ( "node_9", "node_10", 16 ) );
  x -> AddLink ( CLink ( "node_10", "node_11", 9 ) );
  x -> AddLink ( CLink ( "node_11", "node_12", 5 ) );
  x -> AddLink ( CLink ( "node_12", "node_13", 15.25 ) );
  x -> AddLink ( CLink ( "node_13", "node_14", 15.75 ) );
  x -> AddLink ( CLink ( "node_14", "node_15", 18.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_11", 5.25 ) );
  x -> AddLink ( CLink ( "node_14", "node_10", 16.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_8", 2.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_14", 17.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_9", 15 ) );
  x -> AddLink ( CLink ( "node_10", "node_4", 19.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_5", 5 ) );
  x -> AddLink ( CLink ( "node_15", "node_0", 20.75 ) );
  x -> AddLink ( CLink ( "node_5", "node_3", 11 ) );
  x -> AddLink ( CLink ( "node_15", "node_14", 19 ) );
  x -> AddLink ( CLink ( "node_11", "node_1", 17.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_12", 23.5 ) );
  x -> AddLink ( CLink ( "node_14", "node_13", 5.25 ) );
  x -> AddLink ( CLink ( "node_12", "node_8", 7.75 ) );
  x -> AddLink ( CLink ( "node_13", "node_7", 14.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_4", 21.5 ) );
  x -> AddLink ( CLink ( "node_13", "node_7", 7.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_10", 18.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_9", 12.5 ) );
  x -> AddLink ( CLink ( "node_13", "node_11", 4.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_7", 3.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_5", 8.25 ) );
  x -> AddLink ( CLink ( "node_9", "node_13", 20.5 ) );
  x -> AddLink ( CLink ( "node_7", "node_5", 10 ) );
  x -> AddLink ( CLink ( "node_10", "node_9", 6.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_1", 3 ) );
  x -> AddLink ( CLink ( "node_10", "node_5", 7.5 ) );
  x -> AddLink ( CLink ( "node_5", "node_1", 10.75 ) );
  x -> AddLink ( CLink ( "node_14", "node_9", 7 ) );
  x -> AddLink ( CLink ( "node_10", "node_12", 12.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_14", 15.75 ) );
  x -> AddLink ( CLink ( "node_8", "node_3", 14 ) );
  x -> AddLink ( CLink ( "node_6", "node_13", 10 ) );
  x -> AddLink ( CLink ( "node_3", "node_5", 18.25 ) );
  x -> AddLink ( CLink ( "node_10", "node_15", 8.75 ) );
  x -> AddLink ( CLink ( "node_14", "node_2", 10.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_12", 19.25 ) );
  x -> AddLink ( CLink ( "node_4", "node_1", 21.5 ) );
  x -> AddLink ( CLink ( "node_11", "node_0", 11.5 ) );
  x -> AddLink ( CLink ( "node_15", "node_11", 5.5 ) );
  x -> AddLink ( CLink ( "node_7", "node_13", 2.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_6", 7.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_12", 3.5 ) );
  x -> AddLink ( CLink ( "node_7", "node_12", 17.25 ) );
  x -> AddLink ( CLink ( "node_7", "node_1", 14.75 ) );
  x -> AddLink ( CLink ( "node_9", "node_8", 15 ) );
  x -> AddLink ( CLink ( "node_5", "node_4", 22.25 ) );
  x -> AddLink ( CLink ( "node_7", "node_2", 15.25 ) );
  x -> AddLink ( CLink ( "node_8", "node_9", 15 ) );
  x -> AddLink ( CLink ( "node_3", "node_13", 2 ) );
  x -> AddLink ( CLink ( "node_1", "node_14", 4.5 ) );
  x -> AddLink ( CLink ( "node_7", "node_3", 20.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_5", 22 ) );
  x -> AddLink ( CLink ( "node_5", "node_13", 16.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_4", 19.5 ) );
  x -> AddLink ( CLink ( "node_10", "node_13", 8 ) );
  x -> AddLink ( CLink ( "node_9", "node_1", 17 ) );
  x -> AddLink ( CLink ( "node_12", "node_11", 5.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_5", 11 ) );
  x -> AddLink ( CLink ( "node_3", "node_6", 13.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_8", 0.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_11", 6.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_8", 23 ) );
  x -> AddLink ( CLink ( "node_10", "node_12", 17.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_10", 0.5 ) );
  x -> AddLink ( CLink ( "node_8", "node_13", 15.5 ) );
  x -> AddLink ( CLink ( "node_10", "node_8", 10.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_11", 20 ) );
  x -> AddLink ( CLink ( "node_10", "node_2", 1.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_10", 5 ) );
  x -> AddLink ( CLink ( "node_9", "node_11", 14.25 ) );
  x -> AddLink ( CLink ( "node_10", "node_13", 4.75 ) );
  x -> AddLink ( CLink ( "node_5", "node_10", 22 ) );
  x -> AddLink ( CLink ( "node_10", "node_5", 22 ) );
  x -> AddLink ( CLink ( "node_8", "node_13", 17 ) );
  x -> AddLink ( CLink ( "node_14", "node_3", 10 ) );
  x -> AddLink ( CLink ( "node_11", "node_13", 13.75 ) );
  x -> AddLink ( CLink ( "node_11", "node_14", 6 ) );
  x -> AddLink ( CLink ( "node_14", "node_8", 23.25 ) );
  x -> AddLink ( CLink ( "node_7", "node_8", 23.5 ) );
  x -> AddLink ( CLink ( "node_12", "node_13", 15 ) );
  x -> AddLink ( CLink ( "node_10", "node_15", 23.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_13", 14.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_12", 19.25 ) );
  x -> AddLink ( CLink ( "node_12", "node_4", 15.25 ) );
  x -> AddLink ( CLink ( "node_7", "node_2", 19.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_6", 15.25 ) );
  x -> AddLink ( CLink ( "node_9", "node_10", 13.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_9", 1.25 ) );
  x -> AddLink ( CLink ( "node_11", "node_8", 20.75 ) );
  x -> AddLink ( CLink ( "node_8", "node_1", 12.5 ) );
  x -> AddLink ( CLink ( "node_8", "node_0", 20.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_10", 11.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_6", 6.5 ) );
  return x;
}
//-------------------------------------------------------------------------------------------------
void               ValidateCenter3                         ( ACenter           x )
{
  assert ( x -> m_Center == "node_11" );
  assert ( x -> m_MaxDelay == 11.5 );
  assert ( x -> m_Delays["node_0"] == 11.5 );
  assert ( x -> m_Delays["node_1"] == 10.5 );
  assert ( x -> m_Delays["node_10"] == 7.25 );
  assert ( x -> m_Delays["node_12"] == 5 );
  assert ( x -> m_Delays["node_13"] == 4.75 );
  assert ( x -> m_Delays["node_14"] == 6 );
  assert ( x -> m_Delays["node_15"] == 5.5 );
  assert ( x -> m_Delays["node_2"] == 8.5 );
  assert ( x -> m_Delays["node_3"] == 6.75 );
  assert ( x -> m_Delays["node_4"] == 6.75 );
  assert ( x -> m_Delays["node_5"] == 5.25 );
  assert ( x -> m_Delays["node_6"] == 6.75 );
  assert ( x -> m_Delays["node_7"] == 7.5 );
  assert ( x -> m_Delays["node_8"] == 9.25 );
  assert ( x -> m_Delays["node_9"] == 9.75 );
}
//-------------------------------------------------------------------------------------------------
ACenter            GenCenter4                              ( void )
{
  ACenter x = make_shared<CCenter> ();
  x -> AddLink ( CLink ( "node_0", "node_1", 0.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 18 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 14.5 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 20.25 ) );
  x -> AddLink ( CLink ( "node_4", "node_5", 14.75 ) );
  x -> AddLink ( CLink ( "node_5", "node_6", 11.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_7", 12 ) );
  x -> AddLink ( CLink ( "node_7", "node_8", 24.75 ) );
  x -> AddLink ( CLink ( "node_8", "node_9", 1.75 ) );
  x -> AddLink ( CLink ( "node_9", "node_10", 2.25 ) );
  x -> AddLink ( CLink ( "node_10", "node_11", 16 ) );
  x -> AddLink ( CLink ( "node_11", "node_12", 15 ) );
  x -> AddLink ( CLink ( "node_12", "node_13", 5.75 ) );
  x -> AddLink ( CLink ( "node_13", "node_14", 5.75 ) );
  x -> AddLink ( CLink ( "node_14", "node_15", 15.75 ) );
  x -> AddLink ( CLink ( "node_15", "node_16", 23 ) );
  x -> AddLink ( CLink ( "node_16", "node_17", 18.5 ) );
  x -> AddLink ( CLink ( "node_17", "node_18", 11.75 ) );
  x -> AddLink ( CLink ( "node_18", "node_19", 11 ) );
  x -> AddLink ( CLink ( "node_19", "node_20", 21.25 ) );
  x -> AddLink ( CLink ( "node_20", "node_21", 23.75 ) );
  x -> AddLink ( CLink ( "node_21", "node_22", 23.5 ) );
  x -> AddLink ( CLink ( "node_22", "node_23", 22.5 ) );
  x -> AddLink ( CLink ( "node_23", "node_24", 19.25 ) );
  x -> AddLink ( CLink ( "node_8", "node_13", 5.75 ) );
  x -> AddLink ( CLink ( "node_11", "node_23", 11.75 ) );
  x -> AddLink ( CLink ( "node_22", "node_24", 5 ) );
  x -> AddLink ( CLink ( "node_11", "node_19", 19.25 ) );
  x -> AddLink ( CLink ( "node_22", "node_6", 19 ) );
  x -> AddLink ( CLink ( "node_24", "node_8", 10.25 ) );
  x -> AddLink ( CLink ( "node_14", "node_13", 15.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_11", 9.25 ) );
  x -> AddLink ( CLink ( "node_16", "node_22", 5.5 ) );
  x -> AddLink ( CLink ( "node_15", "node_21", 3 ) );
  x -> AddLink ( CLink ( "node_9", "node_4", 16.25 ) );
  x -> AddLink ( CLink ( "node_14", "node_16", 15 ) );
  x -> AddLink ( CLink ( "node_1", "node_14", 14.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_0", 8.75 ) );
  x -> AddLink ( CLink ( "node_0", "node_23", 15 ) );
  x -> AddLink ( CLink ( "node_19", "node_22", 23.25 ) );
  x -> AddLink ( CLink ( "node_4", "node_11", 12.25 ) );
  x -> AddLink ( CLink ( "node_19", "node_15", 24 ) );
  x -> AddLink ( CLink ( "node_3", "node_7", 22 ) );
  x -> AddLink ( CLink ( "node_9", "node_22", 18.75 ) );
  x -> AddLink ( CLink ( "node_11", "node_6", 23.5 ) );
  x -> AddLink ( CLink ( "node_3", "node_21", 15.75 ) );
  x -> AddLink ( CLink ( "node_17", "node_23", 5 ) );
  x -> AddLink ( CLink ( "node_7", "node_4", 5.25 ) );
  x -> AddLink ( CLink ( "node_15", "node_4", 3.5 ) );
  x -> AddLink ( CLink ( "node_5", "node_23", 0.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_2", 11.75 ) );
  x -> AddLink ( CLink ( "node_16", "node_22", 2.75 ) );
  x -> AddLink ( CLink ( "node_15", "node_1", 10 ) );
  x -> AddLink ( CLink ( "node_12", "node_10", 7.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_22", 14.25 ) );
  x -> AddLink ( CLink ( "node_4", "node_0", 11 ) );
  x -> AddLink ( CLink ( "node_20", "node_18", 9 ) );
  x -> AddLink ( CLink ( "node_24", "node_0", 13.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_16", 17.5 ) );
  x -> AddLink ( CLink ( "node_8", "node_22", 16.25 ) );
  x -> AddLink ( CLink ( "node_8", "node_1", 19.25 ) );
  x -> AddLink ( CLink ( "node_20", "node_17", 17 ) );
  x -> AddLink ( CLink ( "node_22", "node_7", 18.75 ) );
  x -> AddLink ( CLink ( "node_7", "node_20", 5 ) );
  x -> AddLink ( CLink ( "node_14", "node_1", 2.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_6", 3.75 ) );
  x -> AddLink ( CLink ( "node_14", "node_1", 22.25 ) );
  x -> AddLink ( CLink ( "node_23", "node_1", 23 ) );
  x -> AddLink ( CLink ( "node_11", "node_6", 14.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_14", 12 ) );
  x -> AddLink ( CLink ( "node_20", "node_23", 13.25 ) );
  x -> AddLink ( CLink ( "node_14", "node_18", 6 ) );
  x -> AddLink ( CLink ( "node_6", "node_15", 13.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_23", 9 ) );
  x -> AddLink ( CLink ( "node_5", "node_13", 10.25 ) );
  x -> AddLink ( CLink ( "node_7", "node_16", 16.25 ) );
  x -> AddLink ( CLink ( "node_11", "node_6", 17.5 ) );
  x -> AddLink ( CLink ( "node_8", "node_5", 19 ) );
  x -> AddLink ( CLink ( "node_6", "node_17", 0.75 ) );
  x -> AddLink ( CLink ( "node_15", "node_8", 16.75 ) );
  x -> AddLink ( CLink ( "node_13", "node_21", 6.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_1", 13 ) );
  x -> AddLink ( CLink ( "node_22", "node_15", 13.5 ) );
  x -> AddLink ( CLink ( "node_20", "node_23", 18.5 ) );
  x -> AddLink ( CLink ( "node_1", "node_0", 0.5 ) );
  x -> AddLink ( CLink ( "node_12", "node_7", 17.75 ) );
  x -> AddLink ( CLink ( "node_20", "node_12", 11.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_4", 12.25 ) );
  x -> AddLink ( CLink ( "node_23", "node_1", 2.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_17", 15.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_24", 21.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_11", 19 ) );
  x -> AddLink ( CLink ( "node_19", "node_0", 14.5 ) );
  x -> AddLink ( CLink ( "node_23", "node_2", 19.75 ) );
  x -> AddLink ( CLink ( "node_23", "node_2", 7 ) );
  x -> AddLink ( CLink ( "node_5", "node_20", 2.75 ) );
  x -> AddLink ( CLink ( "node_18", "node_6", 4.5 ) );
  x -> AddLink ( CLink ( "node_23", "node_19", 2.75 ) );
  x -> AddLink ( CLink ( "node_24", "node_21", 8.75 ) );
  x -> AddLink ( CLink ( "node_17", "node_11", 11 ) );
  x -> AddLink ( CLink ( "node_16", "node_8", 15 ) );
  x -> AddLink ( CLink ( "node_3", "node_2", 9.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_16", 3.25 ) );
  x -> AddLink ( CLink ( "node_22", "node_14", 5.5 ) );
  x -> AddLink ( CLink ( "node_16", "node_13", 8 ) );
  x -> AddLink ( CLink ( "node_23", "node_19", 3.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_12", 10.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_11", 4.25 ) );
  x -> AddLink ( CLink ( "node_7", "node_10", 0.5 ) );
  x -> AddLink ( CLink ( "node_16", "node_3", 11.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_19", 19.75 ) );
  x -> AddLink ( CLink ( "node_17", "node_22", 21.5 ) );
  x -> AddLink ( CLink ( "node_13", "node_4", 23 ) );
  x -> AddLink ( CLink ( "node_2", "node_9", 15 ) );
  x -> AddLink ( CLink ( "node_15", "node_17", 13.5 ) );
  x -> AddLink ( CLink ( "node_10", "node_20", 14.5 ) );
  x -> AddLink ( CLink ( "node_23", "node_5", 19.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_20", 9.75 ) );
  x -> AddLink ( CLink ( "node_18", "node_23", 21.25 ) );
  x -> AddLink ( CLink ( "node_20", "node_17", 16 ) );
  x -> AddLink ( CLink ( "node_12", "node_15", 12.75 ) );
  x -> AddLink ( CLink ( "node_14", "node_16", 1 ) );
  x -> AddLink ( CLink ( "node_18", "node_14", 3.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 19.5 ) );
  x -> AddLink ( CLink ( "node_21", "node_18", 5 ) );
  x -> AddLink ( CLink ( "node_16", "node_7", 3 ) );
  x -> AddLink ( CLink ( "node_22", "node_2", 12.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_4", 7.5 ) );
  x -> AddLink ( CLink ( "node_16", "node_23", 5.75 ) );
  x -> AddLink ( CLink ( "node_12", "node_19", 23.25 ) );
  x -> AddLink ( CLink ( "node_3", "node_7", 14 ) );
  x -> AddLink ( CLink ( "node_16", "node_21", 6 ) );
  x -> AddLink ( CLink ( "node_16", "node_15", 20.75 ) );
  x -> AddLink ( CLink ( "node_20", "node_19", 1 ) );
  x -> AddLink ( CLink ( "node_14", "node_15", 19 ) );
  x -> AddLink ( CLink ( "node_19", "node_7", 2.25 ) );
  x -> AddLink ( CLink ( "node_22", "node_4", 4.75 ) );
  x -> AddLink ( CLink ( "node_8", "node_11", 9.75 ) );
  x -> AddLink ( CLink ( "node_16", "node_2", 8.25 ) );
  x -> AddLink ( CLink ( "node_21", "node_15", 3 ) );
  x -> AddLink ( CLink ( "node_20", "node_18", 10.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_14", 2 ) );
  x -> AddLink ( CLink ( "node_22", "node_10", 22 ) );
  x -> AddLink ( CLink ( "node_16", "node_11", 12 ) );
  x -> AddLink ( CLink ( "node_7", "node_5", 6.5 ) );
  x -> AddLink ( CLink ( "node_14", "node_7", 3.5 ) );
  x -> AddLink ( CLink ( "node_18", "node_12", 12.25 ) );
  x -> AddLink ( CLink ( "node_4", "node_21", 3.75 ) );
  x -> AddLink ( CLink ( "node_7", "node_4", 0.5 ) );
  x -> AddLink ( CLink ( "node_22", "node_7", 20.25 ) );
  x -> AddLink ( CLink ( "node_16", "node_17", 4.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_23", 19.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_21", 5.5 ) );
  x -> AddLink ( CLink ( "node_24", "node_13", 16.75 ) );
  x -> AddLink ( CLink ( "node_11", "node_20", 22.25 ) );
  x -> AddLink ( CLink ( "node_17", "node_10", 5 ) );
  x -> AddLink ( CLink ( "node_20", "node_3", 17 ) );
  x -> AddLink ( CLink ( "node_13", "node_11", 16 ) );
  x -> AddLink ( CLink ( "node_17", "node_11", 13.75 ) );
  x -> AddLink ( CLink ( "node_0", "node_6", 5 ) );
  x -> AddLink ( CLink ( "node_17", "node_11", 6.25 ) );
  x -> AddLink ( CLink ( "node_16", "node_5", 9 ) );
  x -> AddLink ( CLink ( "node_12", "node_10", 8.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 20.25 ) );
  x -> AddLink ( CLink ( "node_21", "node_24", 13.25 ) );
  x -> AddLink ( CLink ( "node_7", "node_4", 9.25 ) );
  x -> AddLink ( CLink ( "node_10", "node_21", 17 ) );
  x -> AddLink ( CLink ( "node_19", "node_9", 3.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_2", 15 ) );
  x -> AddLink ( CLink ( "node_24", "node_2", 21.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_20", 8 ) );
  x -> AddLink ( CLink ( "node_9", "node_11", 13.5 ) );
  x -> AddLink ( CLink ( "node_18", "node_24", 23.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_0", 1.75 ) );
  x -> AddLink ( CLink ( "node_0", "node_10", 5 ) );
  x -> AddLink ( CLink ( "node_5", "node_19", 15.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_11", 10.5 ) );
  x -> AddLink ( CLink ( "node_11", "node_15", 20.75 ) );
  x -> AddLink ( CLink ( "node_14", "node_5", 20 ) );
  x -> AddLink ( CLink ( "node_16", "node_1", 24 ) );
  x -> AddLink ( CLink ( "node_11", "node_9", 9.25 ) );
  x -> AddLink ( CLink ( "node_23", "node_22", 3 ) );
  x -> AddLink ( CLink ( "node_22", "node_21", 5 ) );
  x -> AddLink ( CLink ( "node_2", "node_20", 24 ) );
  x -> AddLink ( CLink ( "node_12", "node_0", 5 ) );
  x -> AddLink ( CLink ( "node_7", "node_15", 6.75 ) );
  x -> AddLink ( CLink ( "node_18", "node_0", 18 ) );
  x -> AddLink ( CLink ( "node_8", "node_21", 7.25 ) );
  x -> AddLink ( CLink ( "node_13", "node_16", 23.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_0", 24.75 ) );
  x -> AddLink ( CLink ( "node_9", "node_23", 2.75 ) );
  x -> AddLink ( CLink ( "node_6", "node_20", 7.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_18", 9.75 ) );
  x -> AddLink ( CLink ( "node_0", "node_17", 22.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_22", 4.75 ) );
  x -> AddLink ( CLink ( "node_15", "node_3", 23 ) );
  x -> AddLink ( CLink ( "node_16", "node_21", 7.25 ) );
  x -> AddLink ( CLink ( "node_13", "node_3", 20.75 ) );
  x -> AddLink ( CLink ( "node_5", "node_2", 11.5 ) );
  x -> AddLink ( CLink ( "node_21", "node_13", 11.75 ) );
  x -> AddLink ( CLink ( "node_21", "node_22", 9.75 ) );
  x -> AddLink ( CLink ( "node_23", "node_4", 5 ) );
  x -> AddLink ( CLink ( "node_6", "node_9", 22.75 ) );
  x -> AddLink ( CLink ( "node_15", "node_9", 15.5 ) );
  x -> AddLink ( CLink ( "node_12", "node_9", 12.75 ) );
  x -> AddLink ( CLink ( "node_17", "node_0", 16.25 ) );
  x -> AddLink ( CLink ( "node_15", "node_17", 13 ) );
  x -> AddLink ( CLink ( "node_22", "node_6", 17 ) );
  x -> AddLink ( CLink ( "node_18", "node_11", 19.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_8", 8.5 ) );
  x -> AddLink ( CLink ( "node_15", "node_4", 6.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_3", 10.5 ) );
  x -> AddLink ( CLink ( "node_5", "node_9", 19.5 ) );
  x -> AddLink ( CLink ( "node_3", "node_0", 4 ) );
  x -> AddLink ( CLink ( "node_18", "node_13", 13.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_5", 13.75 ) );
  x -> AddLink ( CLink ( "node_22", "node_21", 6.25 ) );
  x -> AddLink ( CLink ( "node_10", "node_18", 12 ) );
  x -> AddLink ( CLink ( "node_2", "node_11", 23.5 ) );
  x -> AddLink ( CLink ( "node_22", "node_16", 7 ) );
  x -> AddLink ( CLink ( "node_5", "node_6", 11.25 ) );
  x -> AddLink ( CLink ( "node_11", "node_7", 14.75 ) );
  x -> AddLink ( CLink ( "node_21", "node_12", 24.25 ) );
  x -> AddLink ( CLink ( "node_24", "node_19", 10 ) );
  x -> AddLink ( CLink ( "node_12", "node_8", 16 ) );
  x -> AddLink ( CLink ( "node_18", "node_21", 13.5 ) );
  x -> AddLink ( CLink ( "node_14", "node_2", 23.75 ) );
  x -> AddLink ( CLink ( "node_8", "node_14", 1.5 ) );
  x -> AddLink ( CLink ( "node_19", "node_12", 23 ) );
  x -> AddLink ( CLink ( "node_10", "node_19", 3.75 ) );
  x -> AddLink ( CLink ( "node_17", "node_5", 15 ) );
  x -> AddLink ( CLink ( "node_0", "node_20", 12 ) );
  x -> AddLink ( CLink ( "node_13", "node_19", 3.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_19", 23 ) );
  x -> AddLink ( CLink ( "node_13", "node_7", 6.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_0", 3 ) );
  x -> AddLink ( CLink ( "node_18", "node_15", 5.5 ) );
  return x;
}
//-------------------------------------------------------------------------------------------------
void               ValidateCenter4                         ( ACenter           x )
{
  assert ( x -> m_Center == "node_14" );
  assert ( x -> m_MaxDelay == 9 );
  assert ( x -> m_Delays["node_0"] == 3.25 );
  assert ( x -> m_Delays["node_1"] == 2.75 );
  assert ( x -> m_Delays["node_10"] == 4 );
  assert ( x -> m_Delays["node_11"] == 9 );
  assert ( x -> m_Delays["node_12"] == 8.25 );
  assert ( x -> m_Delays["node_13"] == 5.75 );
  assert ( x -> m_Delays["node_15"] == 7.5 );
  assert ( x -> m_Delays["node_16"] == 1 );
  assert ( x -> m_Delays["node_17"] == 2.75 );
  assert ( x -> m_Delays["node_18"] == 3.75 );
  assert ( x -> m_Delays["node_19"] == 5.75 );
  assert ( x -> m_Delays["node_2"] == 5 );
  assert ( x -> m_Delays["node_20"] == 6.75 );
  assert ( x -> m_Delays["node_21"] == 7 );
  assert ( x -> m_Delays["node_22"] == 3.75 );
  assert ( x -> m_Delays["node_23"] == 5.25 );
  assert ( x -> m_Delays["node_24"] == 8.75 );
  assert ( x -> m_Delays["node_3"] == 4.25 );
  assert ( x -> m_Delays["node_4"] == 4 );
  assert ( x -> m_Delays["node_5"] == 6 );
  assert ( x -> m_Delays["node_6"] == 2 );
  assert ( x -> m_Delays["node_7"] == 3.5 );
  assert ( x -> m_Delays["node_8"] == 1.5 );
  assert ( x -> m_Delays["node_9"] == 3.25 );
}
//-------------------------------------------------------------------------------------------------
ARedundancy        GenRedundancy0                          ( void )
{
  ARedundancy x = make_shared<CRedundancy> ( "node_0");
  x -> AddLink ( CLink ( "node_0", "node_1", 20 ) );
  x -> AddLink ( CLink ( "node_0", "node_5", 5 ) );
  x -> AddLink ( CLink ( "node_0", "node_2", 10 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 10 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 10 ) );
  x -> AddLink ( CLink ( "node_4", "node_5", 5 ) );
  x -> AddLink ( CLink ( "node_4", "node_8", 5 ) );
  x -> AddLink ( CLink ( "node_5", "node_6", 10 ) );
  x -> AddLink ( CLink ( "node_6", "node_7", 10 ) );
  x -> AddLink ( CLink ( "node_7", "node_8", 10 ) );
  return x;
}
//-------------------------------------------------------------------------------------------------
void               ValidateRedundancy0                     ( ARedundancy       x )
{
  assert ( x -> m_Redundancy["node_1"] == 1 );
  assert ( x -> m_Redundancy["node_2"] == 2 );
  assert ( x -> m_Redundancy["node_3"] == 2 );
  assert ( x -> m_Redundancy["node_4"] == 2 );
  assert ( x -> m_Redundancy["node_5"] == 2 );
  assert ( x -> m_Redundancy["node_6"] == 2 );
  assert ( x -> m_Redundancy["node_7"] == 2 );
  assert ( x -> m_Redundancy["node_8"] == 2 );
}
//-------------------------------------------------------------------------------------------------
ARedundancy        GenRedundancy1                          ( void )
{
  ARedundancy x = make_shared<CRedundancy> ( "node_1");
  x -> AddLink ( CLink ( "node_0", "node_1", 23.5 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 19.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 18 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 18.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_3", 4.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_3", 21.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_0", 21.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_4", 24.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 22.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_1", 1.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_0", 16.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 14.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 8 ) );
  x -> AddLink ( CLink ( "node_2", "node_0", 11.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 8.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_1", 5.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_0", 2.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_3", 24.5 ) );
  x -> AddLink ( CLink ( "node_1", "node_3", 4 ) );
  x -> AddLink ( CLink ( "node_2", "node_4", 10.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_1", 1.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_3", 9 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 20.5 ) );
  x -> AddLink ( CLink ( "node_1", "node_3", 4 ) );
  x -> AddLink ( CLink ( "node_1", "node_4", 8.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_0", 23 ) );
  x -> AddLink ( CLink ( "node_2", "node_0", 2 ) );
  x -> AddLink ( CLink ( "node_1", "node_0", 8.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_1", 22.25 ) );
  return x;
}
//-------------------------------------------------------------------------------------------------
void               ValidateRedundancy1                     ( ARedundancy       x )
{
  assert ( x -> m_Redundancy["node_0"] == 13 );
  assert ( x -> m_Redundancy["node_2"] == 10 );
  assert ( x -> m_Redundancy["node_3"] == 12 );
  assert ( x -> m_Redundancy["node_4"] == 9 );
}
//-------------------------------------------------------------------------------------------------
ARedundancy        GenRedundancy2                          ( void )
{
  ARedundancy x = make_shared<CRedundancy> ( "node_3");
  x -> AddLink ( CLink ( "node_0", "node_1", 16.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 12.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 9 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 14 ) );
  x -> AddLink ( CLink ( "node_4", "node_5", 20 ) );
  x -> AddLink ( CLink ( "node_5", "node_6", 10 ) );
  x -> AddLink ( CLink ( "node_5", "node_3", 18.5 ) );
  x -> AddLink ( CLink ( "node_3", "node_2", 14 ) );
  x -> AddLink ( CLink ( "node_5", "node_0", 17.5 ) );
  x -> AddLink ( CLink ( "node_1", "node_0", 1.5 ) );
  x -> AddLink ( CLink ( "node_5", "node_0", 24 ) );
  x -> AddLink ( CLink ( "node_2", "node_1", 1.25 ) );
  x -> AddLink ( CLink ( "node_4", "node_1", 9.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_3", 6.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_1", 18.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_5", 13.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 2.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_6", 11.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_5", 13.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_6", 13.5 ) );
  x -> AddLink ( CLink ( "node_3", "node_5", 14.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_0", 18.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 1 ) );
  x -> AddLink ( CLink ( "node_5", "node_2", 14.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_3", 8.5 ) );
  x -> AddLink ( CLink ( "node_3", "node_0", 10.5 ) );
  x -> AddLink ( CLink ( "node_3", "node_6", 21.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_5", 10.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_0", 10.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_1", 23.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_2", 18.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_6", 20 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 15 ) );
  x -> AddLink ( CLink ( "node_3", "node_1", 23 ) );
  x -> AddLink ( CLink ( "node_6", "node_1", 8.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_1", 5.5 ) );
  x -> AddLink ( CLink ( "node_5", "node_6", 15.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_0", 1 ) );
  x -> AddLink ( CLink ( "node_3", "node_2", 24.25 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 18 ) );
  x -> AddLink ( CLink ( "node_6", "node_3", 13.25 ) );
  x -> AddLink ( CLink ( "node_3", "node_0", 3.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_1", 1.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_3", 10 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 15.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 5.75 ) );
  x -> AddLink ( CLink ( "node_5", "node_0", 19 ) );
  x -> AddLink ( CLink ( "node_2", "node_0", 20.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 4 ) );
  x -> AddLink ( CLink ( "node_2", "node_6", 19 ) );
  x -> AddLink ( CLink ( "node_0", "node_6", 6 ) );
  x -> AddLink ( CLink ( "node_4", "node_3", 8.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_3", 15.75 ) );
  x -> AddLink ( CLink ( "node_5", "node_3", 3 ) );
  x -> AddLink ( CLink ( "node_1", "node_0", 18.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_3", 5.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_1", 7 ) );
  x -> AddLink ( CLink ( "node_5", "node_4", 8 ) );
  x -> AddLink ( CLink ( "node_3", "node_6", 19.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_1", 19.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_2", 19.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_6", 7.25 ) );
  return x;
}
//-------------------------------------------------------------------------------------------------
void               ValidateRedundancy2                     ( ARedundancy       x )
{
  assert ( x -> m_Redundancy["node_0"] == 19 );
  assert ( x -> m_Redundancy["node_1"] == 15 );
  assert ( x -> m_Redundancy["node_2"] == 15 );
  assert ( x -> m_Redundancy["node_4"] == 14 );
  assert ( x -> m_Redundancy["node_5"] == 16 );
  assert ( x -> m_Redundancy["node_6"] == 15 );
}
//-------------------------------------------------------------------------------------------------
ARedundancy        GenRedundancy3                          ( void )
{
  ARedundancy x = make_shared<CRedundancy> ( "node_7");
  x -> AddLink ( CLink ( "node_0", "node_1", 15.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_2", 20.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_3", 16 ) );
  x -> AddLink ( CLink ( "node_3", "node_4", 9.25 ) );
  x -> AddLink ( CLink ( "node_4", "node_5", 9.75 ) );
  x -> AddLink ( CLink ( "node_5", "node_6", 18.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_7", 12.75 ) );
  x -> AddLink ( CLink ( "node_7", "node_8", 12.75 ) );
  x -> AddLink ( CLink ( "node_8", "node_9", 11.75 ) );
  x -> AddLink ( CLink ( "node_9", "node_10", 6.5 ) );
  x -> AddLink ( CLink ( "node_10", "node_7", 5.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_10", 23.5 ) );
  x -> AddLink ( CLink ( "node_9", "node_2", 12.25 ) );
  x -> AddLink ( CLink ( "node_8", "node_0", 4.75 ) );
  x -> AddLink ( CLink ( "node_0", "node_9", 15.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_6", 7.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_10", 8.25 ) );
  x -> AddLink ( CLink ( "node_9", "node_8", 23.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_1", 17.5 ) );
  x -> AddLink ( CLink ( "node_8", "node_6", 4.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_6", 21 ) );
  x -> AddLink ( CLink ( "node_2", "node_8", 19.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_7", 1.5 ) );
  x -> AddLink ( CLink ( "node_7", "node_4", 2.75 ) );
  x -> AddLink ( CLink ( "node_9", "node_5", 23.5 ) );
  x -> AddLink ( CLink ( "node_5", "node_9", 14.5 ) );
  x -> AddLink ( CLink ( "node_8", "node_1", 12.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_0", 7 ) );
  x -> AddLink ( CLink ( "node_3", "node_8", 1 ) );
  x -> AddLink ( CLink ( "node_10", "node_9", 1.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_8", 7 ) );
  x -> AddLink ( CLink ( "node_3", "node_5", 11 ) );
  x -> AddLink ( CLink ( "node_10", "node_6", 2.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_7", 23 ) );
  x -> AddLink ( CLink ( "node_9", "node_6", 9.75 ) );
  x -> AddLink ( CLink ( "node_7", "node_2", 4 ) );
  x -> AddLink ( CLink ( "node_8", "node_7", 6 ) );
  x -> AddLink ( CLink ( "node_2", "node_0", 5.75 ) );
  x -> AddLink ( CLink ( "node_0", "node_1", 4 ) );
  x -> AddLink ( CLink ( "node_9", "node_1", 16.5 ) );
  x -> AddLink ( CLink ( "node_7", "node_4", 24.5 ) );
  x -> AddLink ( CLink ( "node_1", "node_9", 23.75 ) );
  x -> AddLink ( CLink ( "node_8", "node_10", 8.5 ) );
  x -> AddLink ( CLink ( "node_4", "node_9", 4.75 ) );
  x -> AddLink ( CLink ( "node_0", "node_2", 21.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_4", 16.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_7", 12.25 ) );
  x -> AddLink ( CLink ( "node_9", "node_10", 14.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_10", 17.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_9", 21.5 ) );
  x -> AddLink ( CLink ( "node_9", "node_5", 7.5 ) );
  x -> AddLink ( CLink ( "node_9", "node_7", 4 ) );
  x -> AddLink ( CLink ( "node_8", "node_4", 3 ) );
  x -> AddLink ( CLink ( "node_1", "node_9", 24.5 ) );
  x -> AddLink ( CLink ( "node_3", "node_9", 6.5 ) );
  x -> AddLink ( CLink ( "node_7", "node_9", 2.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_2", 22.25 ) );
  x -> AddLink ( CLink ( "node_10", "node_1", 15 ) );
  x -> AddLink ( CLink ( "node_1", "node_10", 11.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_4", 18.5 ) );
  x -> AddLink ( CLink ( "node_9", "node_1", 22.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_5", 24.75 ) );
  x -> AddLink ( CLink ( "node_8", "node_3", 24.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_2", 6 ) );
  x -> AddLink ( CLink ( "node_2", "node_4", 22.5 ) );
  x -> AddLink ( CLink ( "node_0", "node_5", 5.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_5", 11.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_4", 15.25 ) );
  x -> AddLink ( CLink ( "node_9", "node_6", 14.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_6", 1.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_5", 3.5 ) );
  x -> AddLink ( CLink ( "node_10", "node_1", 16 ) );
  x -> AddLink ( CLink ( "node_10", "node_9", 24.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_9", 6.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_0", 17.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_1", 5.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_5", 17 ) );
  x -> AddLink ( CLink ( "node_8", "node_7", 3.25 ) );
  x -> AddLink ( CLink ( "node_3", "node_6", 18 ) );
  x -> AddLink ( CLink ( "node_10", "node_9", 8.5 ) );
  x -> AddLink ( CLink ( "node_10", "node_9", 19.75 ) );
  x -> AddLink ( CLink ( "node_1", "node_8", 22 ) );
  x -> AddLink ( CLink ( "node_7", "node_6", 24.75 ) );
  x -> AddLink ( CLink ( "node_8", "node_7", 17 ) );
  x -> AddLink ( CLink ( "node_9", "node_1", 7.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_9", 0.5 ) );
  x -> AddLink ( CLink ( "node_7", "node_2", 14 ) );
  x -> AddLink ( CLink ( "node_4", "node_1", 10.25 ) );
  x -> AddLink ( CLink ( "node_8", "node_0", 7.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_1", 2.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_9", 21 ) );
  x -> AddLink ( CLink ( "node_1", "node_7", 12.5 ) );
  x -> AddLink ( CLink ( "node_8", "node_6", 7.5 ) );
  x -> AddLink ( CLink ( "node_5", "node_3", 3.75 ) );
  x -> AddLink ( CLink ( "node_3", "node_2", 6.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_8", 2.75 ) );
  x -> AddLink ( CLink ( "node_6", "node_5", 7.5 ) );
  x -> AddLink ( CLink ( "node_1", "node_9", 10.25 ) );
  x -> AddLink ( CLink ( "node_5", "node_6", 12 ) );
  x -> AddLink ( CLink ( "node_9", "node_1", 16.25 ) );
  x -> AddLink ( CLink ( "node_10", "node_6", 14 ) );
  x -> AddLink ( CLink ( "node_8", "node_7", 5 ) );
  x -> AddLink ( CLink ( "node_2", "node_4", 20.25 ) );
  x -> AddLink ( CLink ( "node_6", "node_10", 3.25 ) );
  x -> AddLink ( CLink ( "node_7", "node_2", 8.25 ) );
  x -> AddLink ( CLink ( "node_10", "node_8", 2.75 ) );
  x -> AddLink ( CLink ( "node_0", "node_3", 14 ) );
  x -> AddLink ( CLink ( "node_3", "node_5", 10.5 ) );
  x -> AddLink ( CLink ( "node_8", "node_0", 0.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_9", 4 ) );
  x -> AddLink ( CLink ( "node_9", "node_8", 17.25 ) );
  x -> AddLink ( CLink ( "node_4", "node_5", 9.75 ) );
  x -> AddLink ( CLink ( "node_6", "node_8", 20.75 ) );
  x -> AddLink ( CLink ( "node_4", "node_3", 18.5 ) );
  x -> AddLink ( CLink ( "node_5", "node_0", 23.5 ) );
  x -> AddLink ( CLink ( "node_9", "node_10", 17.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_0", 2 ) );
  x -> AddLink ( CLink ( "node_5", "node_3", 13.75 ) );
  x -> AddLink ( CLink ( "node_10", "node_0", 14.25 ) );
  x -> AddLink ( CLink ( "node_10", "node_2", 10.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_1", 4.25 ) );
  x -> AddLink ( CLink ( "node_8", "node_5", 17 ) );
  x -> AddLink ( CLink ( "node_5", "node_10", 13 ) );
  x -> AddLink ( CLink ( "node_0", "node_8", 16.75 ) );
  x -> AddLink ( CLink ( "node_9", "node_8", 9.75 ) );
  x -> AddLink ( CLink ( "node_9", "node_8", 11.25 ) );
  x -> AddLink ( CLink ( "node_3", "node_1", 24.75 ) );
  x -> AddLink ( CLink ( "node_2", "node_1", 14.25 ) );
  x -> AddLink ( CLink ( "node_1", "node_4", 24.25 ) );
  x -> AddLink ( CLink ( "node_2", "node_5", 3.5 ) );
  x -> AddLink ( CLink ( "node_8", "node_1", 4.5 ) );
  x -> AddLink ( CLink ( "node_2", "node_1", 17.25 ) );
  x -> AddLink ( CLink ( "node_0", "node_9", 18.25 ) );
  x -> AddLink ( CLink ( "node_9", "node_6", 15.5 ) );
  x -> AddLink ( CLink ( "node_6", "node_10", 11.25 ) );
  x -> AddLink ( CLink ( "node_8", "node_5", 10.25 ) );
  return x;
}
//-------------------------------------------------------------------------------------------------
void               ValidateRedundancy3                     ( ARedundancy       x )
{
  assert ( x -> m_Redundancy["node_0"] == 19 );
  assert ( x -> m_Redundancy["node_1"] == 19 );
  assert ( x -> m_Redundancy["node_10"] == 19 );
  assert ( x -> m_Redundancy["node_2"] == 19 );
  assert ( x -> m_Redundancy["node_3"] == 17 );
  assert ( x -> m_Redundancy["node_4"] == 18 );
  assert ( x -> m_Redundancy["node_5"] == 19 );
  assert ( x -> m_Redundancy["node_6"] == 19 );
  assert ( x -> m_Redundancy["node_8"] == 19 );
  assert ( x -> m_Redundancy["node_9"] == 19 );
}
//-------------------------------------------------------------------------------------------------
array<pair<ACenter (*)(void), void (*)(ACenter)>, 5> g_TestsCenter =
{
  make_pair ( GenCenter0, ValidateCenter0 ),
  make_pair ( GenCenter1, ValidateCenter1 ),
  make_pair ( GenCenter2, ValidateCenter2 ),
  make_pair ( GenCenter3, ValidateCenter3 ),
  make_pair ( GenCenter4, ValidateCenter4 )
};
//-------------------------------------------------------------------------------------------------
array<pair<ARedundancy(*)(void), void (*)(ARedundancy)>, 4> g_TestsRedundancy =
{
  make_pair ( GenRedundancy0, ValidateRedundancy0 ),
  make_pair ( GenRedundancy1, ValidateRedundancy1 ),
  make_pair ( GenRedundancy2, ValidateRedundancy2 ),
  make_pair ( GenRedundancy3, ValidateRedundancy3 )
};
