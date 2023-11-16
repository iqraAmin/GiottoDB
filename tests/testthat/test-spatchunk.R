
x = sim_dbPolygonProxy()
y = sim_dbPointsProxy()


test_that('spatial chunking selects the correct number of polys', {
  ext_list = chunk_plan(extent = ext(x), min_chunks = 4L)
  expected_len = nrow(x)

  # chunk data
  chunk_x_list = lapply(
    ext_list,
    function(e) {
      # 'soft' selections on top and right
      extent_filter(x = x, extent = e, include = c(TRUE, TRUE, FALSE, FALSE),
                    method = 'mean')
    }
  )

  chunk_x_list_len = lapply(chunk_x_list, nrow)
  select_len_sum = do.call(sum, chunk_x_list_len)

  expect_equal(select_len_sum, expected_len)
})

test_that('calculateOverlap is working', {
  a <- calculateOverlap(x, y, n_per_chunk = 100)
  expected_len = nrow(y)
  expect_true(nrow(a) == expected_len)

  # ensure .uID values are updated
  v <- a[] %>% dplyr::pull(.uID)
  v <- sort(v)

  expect_true(sum(v == seq(length(v))) == expected_len)
  expect_false(sum(v == sample(seq(length(v)))) == expected_len)
})


