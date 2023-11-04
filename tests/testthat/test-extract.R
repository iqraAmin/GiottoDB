
test_that('Empty bracket extracts data slot', {
  dbDF = new('dbDataFrame')
  expect_identical(dbDF[], dbDF@data)
  # dbDF = disconnect(dbDF)
})
