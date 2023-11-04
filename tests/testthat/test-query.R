
dbDF = new('dbDataFrame')

test_that('queryStack extracts query info from dbDataFrame', {
  expect_identical(queryStack(dbDF), dbDF@data$lazy_query)
})

test_that('queryStack<- replaces query info from dbDataFrame', {
  dbDF = new('dbDataFrame')
  queryStack(dbDF) = 'test'
  expect_identical(queryStack(dbDF), 'test')
})


dbDF = disconnect(dbDF)
